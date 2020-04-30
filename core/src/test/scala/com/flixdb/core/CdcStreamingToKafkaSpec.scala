package com.flixdb.core

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.flixdb.core.postgresql.PostgresSQLDataAccessLayer
import com.typesafe.config.{ConfigParseOptions, ConfigSyntax}
import org.apache.kafka.clients.admin.{CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import org.testcontainers.containers.wait.Wait
import org.testcontainers.containers.{GenericContainer, KafkaContainer}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

abstract class CdcStreamingToKafkaSpec extends BaseCdcStreamingToKafkaSpec {

  val johnEvent1: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = "account-0",
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "John Lennon"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  val johnEvent2: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = "account-0",
    eventType = "com.megacorp.AccountUpgraded",
    sequenceNum = 1,
    data = """{"owner": "John Lennon"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 43L,
    snapshot = false
  )

  val johnEvent3: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"account-0",
    eventType = "com.megacorp.AccountSuspended",
    sequenceNum = 2,
    data = """{"owner": "John Lennon"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  val consumer = new KafkaConsumer[String, String]({
    val consumerProps = kafkaSettings.properties
    consumerProps.put("group.id", "scalatest")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps
  })

  override def beforeAll(): Unit = {
    super.beforeAll()
    import org.apache.kafka.clients.admin.AdminClient

    import scala.jdk.CollectionConverters._
    val props = kafkaSettings.properties
    val admin = AdminClient.create(props)
    val result: CreateTopicsResult = admin.createTopics(List(new NewTopic("flix-accounts", 60, Short.box(1))).asJava)
    result.all().get()
    admin.close(10, TimeUnit.SECONDS)
    consumer.subscribe(List(s"${flixDbConfig.defaultNamespace}-accounts").asJava)
  }

  override def afterAll(): Unit = {
    logger.info("Shutting down Kafka consumer")
    consumer.close(10000, TimeUnit.MILLISECONDS)
    super.afterAll()
  }

  test("Starting the CdcStreamingToKafka extension") {
    val cdcStreamingToKafka = CdcStreamingToKafka(system)
    eventually {
      cdcStreamingToKafka.isStreamRunning.futureValue shouldBe true
    }
  }

  test("Writing some events") {
    val namespace = flixDbConfig.defaultNamespace
    postgreSQL.createTablesIfNotExists(namespace).futureValue shouldBe Done
    postgreSQL.appendEvents(namespace, List(johnEvent1, johnEvent2)).futureValue shouldBe Done
    postgreSQL.appendEvents(namespace, List(johnEvent3)).futureValue shouldBe Done
  }

  test("The events we wrote appear in the Kafka topics") {
    import org.apache.kafka.clients.consumer.ConsumerRecords
    val result = new ArrayBuffer[(String, String)]
    eventually {
      val records: ConsumerRecords[String, String] = consumer.poll(3000) // ms
      val it = records.iterator()
      while (it.hasNext) {
        val n = it.next()
        result.addOne((n.key(), n.value()))
      }
      result.size shouldBe 3
    }
  }

}

class CdcStreamingToKafkaSpecWithPostgreSQL104 extends CdcStreamingToKafkaSpec {
  override def postgreSQLImageName = "sebastianharko/postgres104:latest"
}

class CdcStreamingToKafkaSpecWithPostgreSQL96 extends CdcStreamingToKafkaSpec {
  override def postgreSQLImageName = "sebastianharko/postgres96:latest"
}

class CdcStreamingToKafkaSpecWithPostgreSQL95 extends CdcStreamingToKafkaSpec {
  override def postgreSQLImageName = "sebastianharko/postgres95:latest"
}

abstract class BaseCdcStreamingToKafkaSpec
    extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers
    with Eventually
    with IntegrationPatience {

  import com.typesafe.config.ConfigFactory

  def postgreSQLImageName: String

  val logger = LoggerFactory.getLogger("CdcStreamingToKafkaSpec")

  val postgreSQLContainer = {
    val container = new GenericContainer(postgreSQLImageName)
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

  val testConfig = ConfigFactory
    .parseString(
      s"""
      |container.host = "${postgreSQLContainer.getContainerIpAddress}"
      |container.port = ${postgreSQLContainer.getMappedPort(5432)}
      |postgresql-main-pool.host = $${container.host}
      |postgresql-main-pool.port = $${container.port}
      |postgresql-cdc-pool.host = $${container.host}
      |postgresql-cdc-pool.port = $${container.port}
      |kafka.bootstrap.servers = "${kafkaContainer.getBootstrapServers}"""".stripMargin,
      ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
    )
    .resolve()

  val regularConfig = ConfigFactory.load

  val mergedConfig = testConfig.withFallback(regularConfig)

  implicit val system = ActorSystem("flixdb", config = mergedConfig)

  val kafkaSettings = KafkaSettings(system)

  val flixDbConfig = FlixDbConfiguration(system)

  val postgreSQL = new PostgresSQLDataAccessLayer()(system)

  override def afterAll(): Unit = {
    super.afterAll()
    logger.info("Closing main HikariCP pool")
    postgreSQL.closePools()
    logger.info("Shutting down actor system")
    TestKit.shutdownActorSystem(system, duration = 30.seconds, verifySystemShutdown = true)
    logger.info("Shutting down Kafka container")
    kafkaContainer.stop()
    logger.info("Shutting down PostgreSQL container")
    postgreSQLContainer.stop()
  }

}
