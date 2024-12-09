ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "StreamingDataPipeline",
    libraryDependencies ++= Seq(
      // Kafka dependencies
      "org.apache.kafka" %% "kafka" % "2.8.0",
      "org.apache.kafka" % "kafka-clients" % "2.8.0",

      // Spark dependencies
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",

      // JSON processing and Akka HTTP for integration
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "com.typesafe.akka" %% "akka-http" % "10.2.4",

      // Compatibility for Java 8 in Scala
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1"
    )
  )