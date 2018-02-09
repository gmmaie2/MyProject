name := "kafkaSparkStreamingCassandra"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.2"
//libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.10" % "1.6.2"
//libraryDependencies += "org.apache.spark" % "spark-streaming-flume-sink_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2"
//libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector-java_2.10" % "1.6.0-M1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.10"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.7"
