name := "KafkaHiveConnector"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.storm" % "storm-kafka" % "1.0.1" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.apache.storm" % "storm-core" % "1.0.1" % "provided"
libraryDependencies += "org.apache.storm" % "storm-hive" % "1.0.1" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.apache.storm" % "storm-sql-kafka" % "1.0.1" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.10.0.0" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

