name := "PocSparkWithKafka"

version := "0.1"

scalaVersion :="2.12.4"

val sparkVersion = "2.4.5"
val connectorVersion = "2.4.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

