ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "HomeWork1"
  )

val logbackVersion = "1.3.0-alpha10"
val typesafeConfigVersion = "1.4.2"


resolvers += Resolver.jcenterRepo


// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.4"
libraryDependencies += "com.typesafe" % "config" % typesafeConfigVersion
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.0-alpha5"
