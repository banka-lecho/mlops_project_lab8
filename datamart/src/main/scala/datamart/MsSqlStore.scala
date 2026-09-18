package datamart

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement, Types}
import scala.util.Using

import Protocol._

final class MsSqlStore(spark: SparkSession, cfg: DatasourceConfig) {
  private val runs = s"${cfg.mlSchema}.model_runs"
  private val processed = s"${cfg.rawSchema}.processed_data"
  private val predictions = s"${cfg.mlSchema}.predictions"

  def readFeatures(limit: Option[Int]): DataFrame = {
    val (top, order) = limit.fold(("", ""))(n => (s"TOP $n ", " ORDER BY product_id"))
    val notNull = FeatureColumns.map(c => s"$c IS NOT NULL").mkString(" AND ")
    readQuery(s"SELECT ${top}code, ${FeatureColumns.mkString(", ")} FROM $processed WHERE $notNull$order")
  }

  def writeProcessed(df: DataFrame): Unit =
    df.select("code", FeatureColumns: _*).write
      .format("jdbc")
      .options(jdbcOptions)
      .option("dbtable", processed)
      .option("batchsize", cfg.batchSize.toString)
      .option("truncate", "true")
      .mode("overwrite")
      .save()

  def readPredictions(runId: Int): DataFrame = {
    val features = FeatureColumns.map(c => s"d.$c").mkString(", ")
    readQuery(
      s"SELECT p.code, p.cluster_id, $features FROM $predictions p " +
        s"JOIN $processed d ON d.code = p.code WHERE p.run_id = $runId"
    )
  }

  def writePredictions(runId: Int, rows: Seq[PredictionRow]): Unit = {
    import spark.implicits._
    rows
      .map(r => (runId, r.code, r.clusterId))
      .toDF("run_id", "code", "cluster_id")
      .write
      .format("jdbc")
      .options(jdbcOptions)
      .option("dbtable", predictions)
      .option("batchsize", cfg.batchSize.toString)
      .mode("append")
      .save()
  }

  def runExists(runId: Int): Boolean =
    exists(s"SELECT 1 FROM $runs WHERE run_id = ?", runId)

  def hasPredictions(runId: Int): Boolean =
    exists(s"SELECT TOP 1 1 FROM $predictions WHERE run_id = ?", runId)

  def startRun(req: StartRunRequest): Int =
    withConnection { conn =>
      Using.resource(
        conn.prepareStatement(
          s"INSERT INTO $runs (command, status, started_at, params, scaler_path) " +
            "OUTPUT INSERTED.run_id VALUES (?, 'RUNNING', SYSDATETIME(), ?, ?)"
        )
      ) { st =>
        st.setString(1, req.command)
        st.setString(2, req.params.noSpaces)
        st.setString(3, req.scalerPath)
        Using.resource(st.executeQuery()) { rs =>
          rs.next()
          rs.getInt(1)
        }
      }
    }

  def finishRun(req: FinishRunRequest): Boolean =
    withConnection { conn =>
      Using.resource(
        conn.prepareStatement(
          s"UPDATE $runs SET status = ?, finished_at = SYSDATETIME(), rows_in = ?, best_k = ?, " +
            "best_silhouette = ?, params = COALESCE(?, params), error_message = ?, " +
            "scaler_path = COALESCE(?, scaler_path) WHERE run_id = ?"
        )
      ) { st =>
        st.setString(1, req.status)
        setInt(st, 2, req.rowsIn)
        setInt(st, 3, req.bestK)
        setDouble(st, 4, req.bestSilhouette)
        setString(st, 5, req.params.map(_.noSpaces))
        setString(st, 6, req.errorMessage.map(_.take(1000)))
        setString(st, 7, req.scalerPath)
        st.setInt(8, req.runId)
        st.executeUpdate() > 0
      }
    }

  def getTrainRun(runId: Option[Int]): Option[TrainRun] =
    withConnection { conn =>
      val byId = if (runId.isDefined) " AND run_id = ?" else ""
      Using.resource(
        conn.prepareStatement(
          s"SELECT TOP 1 run_id, JSON_VALUE(params, '$$.model_path'), scaler_path FROM $runs " +
            "WHERE command = 'train' AND status = 'SUCCESS' " +
            s"AND JSON_VALUE(params, '$$.model_path') IS NOT NULL$byId ORDER BY run_id DESC"
        )
      ) { st =>
        runId.foreach(st.setInt(1, _))
        Using.resource(st.executeQuery()) { rs =>
          if (rs.next()) Some(TrainRun(rs.getInt(1), rs.getString(2), rs.getString(3))) else None
        }
      }
    }

  private def jdbcOptions: Map[String, String] = Map(
    "url" -> cfg.jdbcUrl,
    "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "user" -> cfg.user,
    "password" -> cfg.password
  )

  private def readQuery(sql: String): DataFrame =
    spark.read
      .format("jdbc")
      .options(jdbcOptions)
      .option("query", sql)
      .option("fetchsize", cfg.fetchSize.toString)
      .load()

  private def exists(sql: String, id: Int): Boolean =
    withConnection { conn =>
      Using.resource(conn.prepareStatement(sql)) { st =>
        st.setInt(1, id)
        Using.resource(st.executeQuery())(_.next())
      }
    }

  private def withConnection[A](f: Connection => A): A =
    Using.resource(DriverManager.getConnection(cfg.jdbcUrl, cfg.user, cfg.password))(f)

  private def setInt(st: PreparedStatement, i: Int, v: Option[Int]): Unit =
    v.fold(st.setNull(i, Types.INTEGER))(st.setInt(i, _))

  private def setDouble(st: PreparedStatement, i: Int, v: Option[Double]): Unit =
    v.fold(st.setNull(i, Types.DOUBLE))(st.setDouble(i, _))

  private def setString(st: PreparedStatement, i: Int, v: Option[String]): Unit =
    v.fold(st.setNull(i, Types.NVARCHAR))(st.setString(i, _))
}
