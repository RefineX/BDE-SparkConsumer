ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "ConsumerSc212"
  )

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.1"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.3.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8"
)