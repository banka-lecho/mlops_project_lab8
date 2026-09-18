package datamart

import io.circe.Decoder
import io.circe.parser.decode

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.{Codec, Source}
import scala.util.Using

final case class ServerConfig(host: String, port: Int, threads: Int)

final case class SparkConfig(appName: String, master: String, shufflePartitions: Int, logLevel: String)

final case class DataConfig(rawPath: String, interimPath: String, reportPath: String)

final case class SamplingConfig(
    seed: Long,
    rowsPerCore: Int,
    copiesInMemory: Int,
    memoryOverhead: Int,
    maxRows: Int
)

final case class DatasourceConfig(
    host: String,
    port: Int,
    database: String,
    rawSchema: String,
    mlSchema: String,
    fetchSize: Int,
    batchSize: Int,
    user: String,
    password: String
) {
  def jdbcUrl: String =
    s"jdbc:sqlserver://$host:$port;databaseName=$database;encrypt=true;trustServerCertificate=true"

  override def toString: String = s"DatasourceConfig($host:$port/$database)" 
}

final case class MartConfig(
    server: ServerConfig,
    spark: SparkConfig,
    data: DataConfig,
    sampling: SamplingConfig,
    datasource: DatasourceConfig
)

object MartConfig {

  def load(env: Map[String, String] = sys.env): MartConfig = {
    val text = env.get("DATAMART_CONFIG") match {
      case Some(path) => Files.readString(Paths.get(path), StandardCharsets.UTF_8)
      case None       => Using.resource(Source.fromResource("config.json")(Codec.UTF8))(_.mkString)
    }
    val file = decode[FileConfig](text).fold(e => throw new IllegalArgumentException(s"Некорректный конфиг витрины: ${e.getMessage}"), identity)

    val ds = file.datasource
    MartConfig(
      server = file.server,
      spark = file.spark,
      data = file.data,
      sampling = file.sampling,
      datasource = DatasourceConfig(
        host = env.getOrElse("MSSQL_HOST", ds.host),
        port = env.get("MSSQL_PORT").map(_.toInt).getOrElse(ds.port),
        database = ds.database,
        rawSchema = ds.rawSchema,
        mlSchema = ds.mlSchema,
        fetchSize = ds.fetchSize,
        batchSize = ds.batchSize,
        user = required(env, "MSSQL_USER"),
        password = required(env, "MSSQL_PASSWORD")
      )
    )
  }

  private def required(env: Map[String, String], name: String): String =
    env.get(name).filter(_.nonEmpty).getOrElse(throw new IllegalArgumentException(s"Задайте переменную окружения $name"))

  private final case class FileDatasource(
      host: String,
      port: Int,
      database: String,
      rawSchema: String,
      mlSchema: String,
      fetchSize: Int,
      batchSize: Int
  )
  private final case class FileConfig(
      server: ServerConfig,
      spark: SparkConfig,
      data: DataConfig,
      sampling: SamplingConfig,
      datasource: FileDatasource
  )

  private implicit val serverDecoder: Decoder[ServerConfig] =
    Decoder.forProduct3("host", "port", "threads")(ServerConfig.apply)
  private implicit val sparkDecoder: Decoder[SparkConfig] =
    Decoder.forProduct4("app_name", "master", "shuffle_partitions", "log_level")(SparkConfig.apply)
  private implicit val datasourceDecoder: Decoder[FileDatasource] =
    Decoder.forProduct7("host", "port", "database", "raw_schema", "ml_schema", "fetch_size", "batch_size")(
      FileDatasource.apply
    )
  private implicit val dataDecoder: Decoder[DataConfig] =
    Decoder.forProduct3("raw_path", "interim_path", "report_path")(DataConfig.apply)
  private implicit val samplingDecoder: Decoder[SamplingConfig] =
    Decoder.forProduct5("seed", "rows_per_core", "copies_in_memory", "memory_overhead", "max_rows")(
      SamplingConfig.apply
    )
  private implicit val fileDecoder: Decoder[FileConfig] =
    Decoder.forProduct5("server", "spark", "data", "sampling", "datasource")(FileConfig.apply)
}
