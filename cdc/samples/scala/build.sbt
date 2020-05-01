organization := "com.flixdb.cdc"

name := "example"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % "2.6.4",
  "com.flixdb" %% "cdc" % "1.0",
  "com.zaxxer" % "HikariCP" % "3.4.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3")