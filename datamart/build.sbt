ThisBuild / scalaVersion := "2.13.18" // та же Scala, с которой собран Spark 4.2.0
ThisBuild / organization := "ru.bigdata"

val sparkVersion = "4.2.0" // совпадает с pyspark в requirements.txt
val circeVersion = "0.14.15"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "datamart",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.1.jre11",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion
    ),
    Compile / mainClass := Some("datamart.DataMartApp"),
    scalacOptions ++= Seq("-deprecation", "-feature"),
    Compile / packageDoc / publishArtifact := false, 
    Universal / javaOptions ++= Seq(
      "-J-XX:+IgnoreUnrecognizedVMOptions",
      "-J--add-opens=java.base/java.lang=ALL-UNNAMED",
      "-J--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "-J--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "-J--add-opens=java.base/java.io=ALL-UNNAMED",
      "-J--add-opens=java.base/java.net=ALL-UNNAMED",
      "-J--add-opens=java.base/java.nio=ALL-UNNAMED",
      "-J--add-opens=java.base/java.util=ALL-UNNAMED",
      "-J--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "-J--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "-J--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "-J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "-J--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "-J--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "-J--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-J-Djdk.reflect.useDirectMethodHandle=false",
      "-J-Dio.netty.tryReflectionSetAccessible=true"
    )
  )
