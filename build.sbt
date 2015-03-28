version       := "1.0"

scalaVersion  := "2.10.4"

// Each test spec creates a Spark context, so we only want one to run at a time
// We expect users to explicitly use 'testOnly' most of the time, anyway
parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "org.apache.spark" %% "spark-core" % "1.1.1" % "provided"
//  "org.apache.spark" %% "spark-sql" % "1.1.1" % "provided",
//  "org.apache.spark" %% "spark-hive" % "1.1.1" % "provided",
//  "org.apache.spark" %% "spark-streaming" % "1.1.1",
//  "org.apache.spark" %% "spark-streaming-kafka" % "1.1.1",
//  "org.apache.spark" %% "spark-streaming-flume" % "1.1.1",
//  "org.apache.spark" %% "spark-mllib" % "1.1.1",
)
