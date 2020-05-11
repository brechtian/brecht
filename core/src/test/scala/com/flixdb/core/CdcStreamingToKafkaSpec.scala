package com.flixdb.core

import java.time.{Duration => JavaDuration}
import java.util.UUID.randomUUID

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.flixdb.core.KafkaEventEnvelope._
import com.flixdb.core.postgresql.PostgresSQLDataAccessLayer
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}
import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import org.testcontainers.containers.wait.strategy
import org.testcontainers.containers.{GenericContainer, KafkaContainer}
import spray.json._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util._

class CdcStreamingToKafkaSpecWithPostgreSQL1012 extends BaseCdcStreamingToKafkaSpec {
  override def postgreSQLImageName =
    "flixdb-docker-images.bintray.io/flixdb/postgresql:10.12"
}

class CdcStreamingToKafkaSpecWithPostgreSQL117 extends BaseCdcStreamingToKafkaSpec {
  override def postgreSQLImageName =
    "flixdb-docker-images.bintray.io/flixdb/postgresql:11.7"
}

class CdcStreamingToKafkaSpecWithPostgreSQL122 extends BaseCdcStreamingToKafkaSpec {
  override def postgreSQLImageName =
    "flixdb-docker-images.bintray.io/flixdb/postgresql:12.2"
}

abstract class BaseCdcStreamingToKafkaSpec
    extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers
    with Eventually
    with IntegrationPatience {

  def postgreSQLImageName: String

  val logger = LoggerFactory.getLogger("CdcStreamingToKafkaSpec")

  val postgreSQLContainer = {
    val container = new GenericContainer(postgreSQLImageName)
    container.waitingFor(strategy.Wait.forLogMessage(".*ready to accept connections.*\\n", 2))
    container.addExposedPort(5432)
    container.start()
    container
  }

  val kafkaContainer: KafkaContainer = {
    val container = new KafkaContainer("4.1.2")
    container.start()
    container
  }

  implicit val system = ActorSystem(
    "flixdb",
    config = ConfigFactory
      .parseString(
        s"""
         |container.host = "${postgreSQLContainer.getContainerIpAddress}"
         |container.port = ${postgreSQLContainer.getMappedPort(5432)}
         |postgresql-main-pool.host = $${container.host}
         |postgresql-main-pool.port = $${container.port}
         |postgresql-cdc-pool.host = $${container.host}
         |postgresql-cdc-pool.port = $${container.port}
         |kafka-extra.bootstrap.servers = "${kafkaContainer.getBootstrapServers}"""".stripMargin,
        ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
      )
      .resolve()
      .withFallback(ConfigFactory.load)
  )

  val kafkaSettings = KafkaConfig(system)

  val flixDbConfig = FlixDbConfig(system)

  val postgreSQL = new PostgresSQLDataAccessLayer()

  val kafkaConsumer = new KafkaConsumer[String, String]({
    val consumerProps = kafkaSettings.extraConfigAsProperties
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "scalatest")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps
  })

  def randomEvents(subStreamId: Int) =
    (1 to 7).map(seqNum =>
      EventEnvelope(
        eventId = randomUUID().toString,
        subStreamId = s"account-${subStreamId}",
        eventType = "com.megacorp.MoneyWithdrawn",
        sequenceNum = seqNum,
        data = s"""{"amount": "${Random.nextInt(100)}"}""",
        stream = "accounts",
        tags = List("megacorp"),
        timestamp = 42L,
        snapshot = false
      )
    )

  override def beforeAll(): Unit = {

    val topicName = flixDbConfig.getTopicName("test", "accounts")

    val props = kafkaSettings.extraConfigAsProperties
    val admin = AdminClient.create(props)
    val result: CreateTopicsResult = admin.createTopics(List(new NewTopic(topicName, 60, Short.box(1))).asJava)
    result.all().get()
    admin.close(JavaDuration.ofSeconds(10))
    kafkaConsumer.subscribe(List(topicName).asJava)
  }

  test("Starting the CdcStreamingToKafka extension") {
    val cdcStreamingToKafka = CdcStreamingToKafka(system)
    eventually {
      cdcStreamingToKafka.isStreamRunning.futureValue shouldBe true
    }
  }

  test("Writing some events") {
    val namespace = "test"
    postgreSQL.createTablesIfNotExists(namespace).futureValue shouldBe Done
    val events: Seq[EventEnvelope] = (1 to 100).flatMap(subStreamId => randomEvents(subStreamId))
    postgreSQL.appendEvents(namespace, events.toList).futureValue shouldBe Done
  }

  case class KafkaMsg(key: String, value: KafkaEventEnvelope, partition: Int)

  def checkResult(result: List[KafkaMsg]): Unit = {
    result should have size 700
    result.groupBy(_.key).foreach {
      case (_, msgs: List[KafkaMsg]) =>
        // check that messages with the same key end up in the same partition
        msgs.map(_.partition).distinct should have size 1
    }
    result.groupBy(_.value.subStreamId).foreach {
      case (_, msgs: List[KafkaMsg]) =>
        msgs.map(_.value).map(_.sequenceNum) shouldBe (1 to 7).toList
    }
  }

  test("Reading Kafka topics") {
    val result = new ArrayBuffer[KafkaMsg]
    eventually {
      val records: ConsumerRecords[String, String] = kafkaConsumer.poll(JavaDuration.ofSeconds(3)) // ms
      val it = records.iterator()
      while (it.hasNext) {
        val kafkaMsg = it.next()
        result.addOne(
          KafkaMsg(
            key = kafkaMsg.key(),
            value = kafkaMsg.value().parseJson.convertTo[KafkaEventEnvelope],
            partition = kafkaMsg.partition()
          )
        )
      }
      checkResult(result.toList)
    }
  }

  override def afterAll(): Unit = {
    logger.info("Shutting down Kafka consumer")
    kafkaConsumer.close(JavaDuration.ofSeconds(10))
    logger.info("Closing main HikariCP pool")
    postgreSQL.closePools()
    logger.info("Shutting down actor system")
    TestKit.shutdownActorSystem(system, duration = 30.seconds, verifySystemShutdown = true)
    logger.info("Shutting down PostgreSQL container")
    postgreSQLContainer.stop()
    logger.info("Shutting down Kafka container")
    kafkaContainer.stop()

  }

}
