ThisBuild / scalaVersion := "2.13.1"

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature")

ThisBuild / parallelExecution in Test := false

ThisBuild / Test / fork := true // specifies that all tests will be executed in a single external JVM

ThisBuild / organization := "com.flixdb"

ThisBuild / publishTo := Some("io.cloudrepo" at "https://flixdb.mycloudrepo.io/repositories/cdc")

ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val scala213 = "2.13.1"
lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala213, scala212)

val akkaVersion = "2.6.4"
val akkaHttpVersion = "10.1.11"

// Dependencies
val slf4j = "org.slf4j" % "slf4j-api" % "1.7.30"
val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
val postgreSQLDriver = "org.postgresql" % "postgresql" % "42.2.12"
val hikariCP = "com.zaxxer" % "HikariCP" % "3.4.2"
val sprayJson = "io.spray" %% "spray-json" % "1.3.5"
val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.6.7"
val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
val akkaHttp = "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
val akkaStreamKafka = "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.2"
val akkaCluster = "com.typesafe.akka" %% "akka-cluster" % akkaVersion
val akkaClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion
val akkaClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion
val akkaDistributedData = "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion
val fastparse = "com.lihaoyi" %% "fastparse" % "2.2.2"
val jackartaXmlBindApi = "jakarta.xml.bind" % "jakarta.xml.bind-api" % "2.3.2"
val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % "2.6.4"
val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1"
val testcontainers = "org.testcontainers" % "testcontainers" % "1.14.1"
val testContainersKafka = "org.testcontainers" % "kafka" % "1.12.4"
val simulacrum = "org.typelevel" %% "simulacrum" % "1.0.0"

lazy val root = (project in file("."))
  .aggregate(core, pb, cdc)
  .settings(
    crossScalaVersions := Nil,
    publish / skip := true
  )

lazy val core = {
  (project in file("core"))
    .settings(
      scalacOptions += "-Ymacro-annotations",
      crossScalaVersions := Nil,
      publish / skip := true,
      name := "core",
      version := "0.1",
      libraryDependencies := Seq(
        logback,
        sprayJson,
        json4sJackson,
        akkaSlf4j,
        akkaHttp,
        akkaHttpSprayJson,
        akkaStream,
        akkaStreamKafka,
        akkaCluster,
        akkaClusterTools,
        akkaDistributedData,
        akkaClusterSharding,
        postgreSQLDriver,
        hikariCP,
        simulacrum,
        scalaTest % Test,
        testContainersKafka % Test,
        akkaStreamTestKit % Test,
        akkaTestKit % Test
      )
    )
    .dependsOn(pb, cdc)
}

lazy val pb = (project in file("pb")).settings(
  publish / skip := true,
  crossScalaVersions := Nil,
  PB.targets in Compile := Seq(
    scalapb.gen(flatPackage = true) -> (sourceManaged in Compile).value
  )
)

lazy val cdc = {
  (project in file("cdc"))
    .settings(
      crossScalaVersions := supportedScalaVersions,
      name := "cdc",
      version := "0.1-ALPHA",
      libraryDependencies := Seq(
        slf4j,
        fastparse,
        akkaStream,
        postgreSQLDriver,
        akkaSlf4j % Test,
        logback % Test,
        hikariCP % Test,
        scalaTest % Test,
        akkaTestKit % Test,
        akkaStreamTestKit % Test,
        jackartaXmlBindApi % Test,
        testcontainers % Test
      )
    )
}
