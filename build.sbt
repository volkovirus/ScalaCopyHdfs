name := "scala_copy_hdfs"

version := "0.1"

//scalaVersion := "2.13.5"
scalaVersion := "2.12.13"

//addSbtPlugin("org.apache.hadoop" % "hadoop-client" % "3.2.2")
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime