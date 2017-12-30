name := "spark-streaming-scala"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.3",
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  "org.apache.spark" %% "spark-hive" % "1.6.3",
  "org.apache.spark" %% "spark-streaming" % "1.6.3",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
  "org.apache.spark" %% "spark-streaming-flume" % "1.6.3",
  "org.apache.spark" %% "spark-mllib" % "1.6.3",
  "org.apache.spark" %% "spark-streaming-flume-sink" % "1.6.3",
  "org.scala-lang" % "scala-library" % "2.10.6",
  "org.apache.commons" % "commons-lang3" % "3.5"
)
    