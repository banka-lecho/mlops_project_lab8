package datamart

import io.circe.Json
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import Protocol._

final class Preprocessor(spark: SparkSession, config: MartConfig, store: MsSqlStore) {
  private val reserveMb = 300

  private val csvNames = Map(
    "energy_kcal_100g" -> "energy-kcal_100g",
    "saturated_fat_100g" -> "saturated-fat_100g"
  )

  def run(request: PreprocessRequest): Json = {
    val rawPath = request.rawPath.getOrElse(config.data.rawPath)
    val maxRows = request.maxRows.getOrElse(config.sampling.maxRows)

    val raw = readRaw(rawPath)
    raw.write.mode("overwrite").parquet(config.data.interimPath)
    println(s"Промежуточные данные сохранены в ${config.data.interimPath}")

    val (cleaned, stats) = clean(spark.read.parquet(config.data.interimPath))
    val totalRows = stats.last._2
    if (totalRows == 0) throw badRequest(s"После очистки не осталось строк ($rawPath)")

    val size = sampleSize(totalRows, maxRows)
    println(s"Итоговая выборка: ${size.targetRows} из ${size.totalRows} строк (ограничение: ${size.limitedBy})")

    store.writeProcessed(drawSample(cleaned, size))

    val report = Json.obj(
      "cleaning" -> Json.fromValues(stats.map { case (step, rows) =>
        Json.obj("step" -> Json.fromString(step), "rows" -> Json.fromLong(rows))
      }),
      "sample" -> Json.obj(
        "total_rows" -> Json.fromLong(size.totalRows),
        "target_rows" -> Json.fromLong(size.targetRows),
        "limited_by" -> Json.fromString(size.limitedBy)
      )
    )
    writeReport(report)
    report
  }

  private def readRaw(path: String): DataFrame = {
    val columns = FeatureColumns.map(name => col(s"`${csvNames.getOrElse(name, name)}`").cast("double").alias(name))
    spark.read
      .option("header", value = true)
      .option("sep", "\t")
      .option("mode", "PERMISSIVE")
      .csv(path)
      .select(col("code") +: columns: _*)
  }

  private def clean(input: DataFrame): (DataFrame, Seq[(String, Long)]) = {
    val steps: Seq[(String, DataFrame => DataFrame)] = Seq(
      "duplicates" -> (df => df.filter(col("code").isNotNull && length(col("code")) <= 64).dropDuplicates("code")),
      "nulls" -> (_.na.drop(FeatureColumns)),
      "negative" -> (df => df.filter(FeatureColumns.map(c => col(c) >= 0).reduce(_ && _))),
      "zeros" -> (df => df.filter(FeatureColumns.map(c => col(c) > 0).reduce(_ || _))),
      "sugar_gt_carbs" -> (_.filter(col("sugars_100g") <= col("carbohydrates_100g"))),
      "macro_sum" -> (_.filter(col("fat_100g") + col("carbohydrates_100g") + col("proteins_100g") <= 100))
    )

    val df = input.cache()
    var current = df
    var stats = Seq("raw" -> df.count())
    steps.foreach { case (name, step) =>
      current = step(current)
      stats = stats :+ (name -> current.count())
    }
    stats.foreach { case (step, rows) => println(s"$step: $rows строк") }
    (current, stats)
  }

  private def sampleSize(totalRows: Long, maxRows: Int): SampleSize = {
    val cores = Runtime.getRuntime.availableProcessors()
    val memoryFraction = spark.conf.get("spark.memory.fraction", "0.6").toDouble
    val storageFraction = spark.conf.get("spark.memory.storageFraction", "0.5").toDouble
    val usableMb = Runtime.getRuntime.maxMemory().toDouble / (1024 * 1024) - reserveMb
    val storageMb = usableMb * memoryFraction * storageFraction

    val cpuTarget = config.sampling.rowsPerCore.toLong * cores
    val bytesPerRow = FeatureColumns.size * 8L * config.sampling.copiesInMemory * config.sampling.memoryOverhead
    val memoryTarget = (storageMb * 1024 * 1024).toLong / bytesPerRow

    val candidates = Seq(
      totalRows -> "все чистые данные",
      cpuTarget -> "CPU",
      memoryTarget -> "память",
      maxRows.toLong -> "потолок из конфига"
    )
    val (targetRows, limitedBy) = candidates.minBy(_._1)
    SampleSize(totalRows, targetRows, limitedBy)
  }

  private def drawSample(df: DataFrame, size: SampleSize): DataFrame =
    if (size.targetRows >= size.totalRows) df
    else {
      val fraction = math.min(1.0, size.targetRows.toDouble / size.totalRows * 1.1)
      df.sample(withReplacement = false, fraction, config.sampling.seed).limit(size.targetRows.toInt)
    }

  private def writeReport(report: Json): Unit = {
    val path = Paths.get(config.data.reportPath)
    Option(path.getParent).foreach(Files.createDirectories(_))
    Files.write(path, report.spaces2.getBytes(StandardCharsets.UTF_8))
    println(s"Отчёт о предобработке сохранён в ${config.data.reportPath}")
  }
}

final case class SampleSize(totalRows: Long, targetRows: Long, limitedBy: String)
