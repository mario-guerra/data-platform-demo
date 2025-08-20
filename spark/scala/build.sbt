ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "1.0"

lazy val root = (project in file("."))
  .settings(
    name := "batch-etl",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      "io.delta" %% "delta-core" % "2.4.0",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    },
    assembly / assemblyJarName := "batch-etl-assembly-1.0.jar"
  )