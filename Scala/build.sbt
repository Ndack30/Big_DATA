name := "hello"
version := "1.0"
scalaVersion := "2.12.15"

resolvers += "MinIO Repository" at "https://dl.min.io/client/spark-select_2.12/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-avro" % "3.0.1",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.1",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "com.amazonaws" % "aws-java-sdk" % "1.11.836"

)
