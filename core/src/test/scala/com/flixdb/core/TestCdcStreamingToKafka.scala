package com.flixdb.core

import java.util.UUID.randomUUID

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.{ConfigParseOptions, ConfigSyntax}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.json4s.JsonAST.JString
import org.json4s.{DefaultFormats, JValue}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.wait.Wait
import org.testcontainers.containers.{GenericContainer, KafkaContainer}

class TestCdcStreamingToKafka extends AnyFunSuiteLike with BeforeAndAfterAll with ScalaFutures with Matchers {

  import com.typesafe.config.ConfigFactory

  val postgreSQLContainer = {
    val container = new GenericContainer("sebastianharko/postgres104:latest")
    container.waitingFor(Wait.forLogMessage(".*ready to accept connections.*\\n", 1))
    container.addExposedPort(5432)
    container.start()
    container
  }

  val kafkaContainer: KafkaContainer = {
    val container = new KafkaContainer("4.1.2")
    container.start()
    container
  }

  val testConfig = ConfigFactory.parseString(
    s"""|container.host = "${postgreSQLContainer.getContainerIpAddress}"
        |container.port = ${postgreSQLContainer.getMappedPort(5432)}
        |postgres.host = $${container.host}
        |postgres.port = $${container.port}
        |postgres-cdc.host = $${container.host}
        |postgres-cdc.port = $${container.port}
        |kafka.bootstrap.servers = "${kafkaContainer.getBootstrapServers}"""".stripMargin,
    ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
  ).resolve()

  val regularConfig = ConfigFactory.load

  val mergedConfig = testConfig.withFallback(regularConfig)

  implicit val system = ActorSystem("flixdb", config = mergedConfig)

  val kafkaSettings = KafkaSettings(system)

  val flixDbConfiguration = FlixDbConfiguration(system)

  val event1: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = "account-0",
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "Silvia Cruz"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L
  )

  val event2: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = "account-0",
    eventType = "com.megacorp.AccountUpgraded",
    sequenceNum = 1,
    data = """{"owner": "Silvia Cruz"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 43L
  )

  val event3: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"account-0",
    eventType = "com.megacorp.AccountSuspended",
    sequenceNum = 2,
    data = """{"owner": "Silvia Cruz"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L
  )


  test("We can start the CdcStreamingToKafka extension") {
    CdcStreamingToKafka(system)
  }

  val postgreSQL = PostgreSQL(system)
  test("We can write some events") {
    Thread.sleep(5000)
    postgreSQL.createTablesIfNotExists("default").futureValue shouldBe Done
    postgreSQL.appendEvents("default", List(event1, event2)).futureValue shouldBe Done
    postgreSQL.appendEvents("default", List(event3)).futureValue shouldBe Done
  }

  test("The events we wrote appear in the change data capture topic in Kafka") {

    Consumer
      .plainSource(
        kafkaSettings.getBaseConsumerSettings.withGroupId("scalatest")
          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
        Subscriptions.topics(flixDbConfiguration.cdcKafkaStreamName)
      )
      .map(_.value())
      .map {
        case json: String => {
          println(json)
          import org.json4s.jackson._
          implicit val formats = DefaultFormats
          val j: JValue = parseJson(json)
          j
        }
      }
      .runWith(TestSink.probe[JValue])
      .request(3)
      // TODO: add additional checks
      .expectNextChainingPF(f = {
        case j: JValue if (j \ "changeType") == JString("RowInserted") =>
      })
      .expectNextChainingPF(f = {
        case j: JValue if (j \ "changeType") == JString("RowInserted") =>
      })
      .expectNextChainingPF(f = {
        case j: JValue if (j \ "changeType") == JString("RowInserted") =>
      })

  }

  override def afterAll(): Unit = {
    super.afterAll()
    postgreSQL.closePools()
    system.terminate()
    kafkaContainer.stop()
    postgreSQLContainer.stop()
  }

}
