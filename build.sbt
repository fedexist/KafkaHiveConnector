name := "KafkaHiveConnector"

version := "1.0"

scalaVersion := "2.12.1"

mainClass := Some("TestTopology")

libraryDependencies += "org.apache.storm" % "storm-kafka" % "1.0.1"
libraryDependencies += "org.apache.storm" % "storm-core" % "1.0.1"
libraryDependencies += "org.apache.storm" % "storm-hive" % "1.0.1"
libraryDependencies += "org.apache.storm" % "storm-sql-kafka" % "1.0.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}