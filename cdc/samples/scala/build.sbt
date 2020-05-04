organization := "com.flixdb.cdc"

name := "samples"

scalaVersion := "2.13.1"

resolvers += Resolver.bintrayRepo("flixdb",
  "maven")

val akkaVersion = "2.6.4"

val akkaHttpVersion = "10.1.11"

libraryDependencies ++= Seq(
  "com.flixdb" %% "cdc" % "0.1-SNAPSHOT",
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.zaxxer" % "HikariCP" % "3.4.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)