ThisBuild / scalaVersion := "2.13.1"

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature")

ThisBuild / parallelExecution in Test := false

ThisBuild / Test / fork := true // specifies that all tests will be executed in a single external JVM

ThisBuild / organization := "com.brecht"

ThisBuild / bintrayOrganization := Some("brechtian")

ThisBuild / resolvers ++= Seq(
  Resolver.bintrayRepo("lonelyplanet", "maven")
)

lazy val scala213 = "2.13.1"
lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala213, scala212)

val akkaVersion = "2.6.5"
val akkaHttpVersion = "10.1.12"

// Dependencies
val slf4j = "org.slf4j" % "slf4j-api" % "1.7.30"
val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
val postgreSQLDriver = "org.postgresql" % "postgresql" % "42.2.12"
val hikariCP = "com.zaxxer" % "HikariCP" % "3.4.5"
val sprayJson = "io.spray" %% "spray-json" % "1.3.5"
val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.6.7"
val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
val akkaHttp = "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
val akkaHttpTestKit = "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion
val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion
val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
val akkaStreamKafka = "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.2"
val akkaCluster = "com.typesafe.akka" %% "akka-cluster" % akkaVersion
val akkaClusterTyped = "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion
val akkaClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion
val akkaClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion
val akkaDistributedData = "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion
val fastparse = "com.lihaoyi" %% "fastparse" % "2.2.2"
val jackartaXmlBindApi = "jakarta.xml.bind" % "jakarta.xml.bind-api" % "2.3.2"
val akkaTypedTestKit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion
val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion
val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1"
val testcontainers = "org.testcontainers" % "testcontainers" % "1.14.1"
val testContainersKafka = "org.testcontainers" % "kafka" % "1.12.4"
val simulacrum = "org.typelevel" %% "simulacrum" % "1.0.0"
val lithium = "com.swissborg" %% "lithium" % "0.11.2"

val prometheusClient = "io.prometheus" % "simpleclient" % "0.8.1"
val prometheusHotspot = "io.prometheus" % "simpleclient_hotspot" % "0.8.1"
val prometheusAkkaHttp = "com.lonelyplanet" %% "prometheus-akka-http" % "0.5.0"

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
        akkaActorTyped,
        akkaSlf4j,
        akkaHttp,
        akkaHttpSprayJson,
        akkaStream,
        akkaStreamKafka,
        akkaCluster,
        lithium,
        akkaClusterTyped,
        akkaClusterTools,
        akkaDistributedData,
        akkaClusterSharding,
        postgreSQLDriver,
        hikariCP,
        simulacrum,
        prometheusClient,
        prometheusHotspot,
        scalaTest % Test,
        testContainersKafka % Test,
        akkaStreamTestKit % Test,
        akkaTestKit % Test,
        akkaTypedTestKit % Test,
        akkaHttpTestKit % Test
      )
    )
    .dependsOn(pb, cdc)
    .enablePlugins(JavaServerAppPackaging)
}

lazy val pb = (project in file("pb")).settings(
  libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
  publish / skip := true,
  crossScalaVersions := Nil,
  PB.targets in Compile := Seq(
    scalapb.gen(flatPackage = true) -> (sourceManaged in Compile).value
  )
)

lazy val cdc = {
  (project in file("cdc"))
    .settings(
      bintrayRepository := "maven",
      licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
      crossScalaVersions := supportedScalaVersions,
      name := "cdc",
      version := "0.1-SNAPSHOT",
      libraryDependencies := Seq(
        slf4j,
        fastparse,
        sprayJson,
        akkaStream,
        akkaActorTyped,
        postgreSQLDriver,
        prometheusClient,
        prometheusHotspot,
        prometheusAkkaHttp,
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
