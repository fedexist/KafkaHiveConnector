name := "KafkaHiveConnector"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.storm" % "storm-kafka" % "1.0.1.2.5.5.0-157" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.apache.storm" % "storm-core" % "1.0.1.2.5.5.0-157" % "provided"
libraryDependencies += "org.apache.storm" % "storm-hive" % "1.0.1.2.5.5.0-157" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.apache.storm" % "storm-sql-kafka" % "1.0.1.2.5.5.0-157" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.10.0.2.5.5.0-157" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3.2.5.5.0-157" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3.2.5.5.0-157" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "org.joda" % "joda-convert" % "1.8.2"
libraryDependencies += "org.apache.storm" % "storm-mongodb" % "1.0.1.2.5.5.0-157" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeper") exclude("org.slf4j", "slf4j-log4j12")



assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", ps @ _*) => MergeStrategy.first
  case x => MergeStrategy.first
}

