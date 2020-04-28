package com.flixdb.core

import java.util.UUID._

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.Wait
import org.testcontainers.containers.wait.strategy.WaitStrategy

import scala.language.postfixOps

class TestPostgreSQLCompatibleDb
    extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with Matchers {

  val log = LoggerFactory.getLogger(classOf[TestPostgreSQLCompatibleDb])

  implicit val defaultPatienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(10, Seconds)),
      interval = scaled(Span(2, Seconds))
    )

  val postgreSQLContainer = {
    val container = new GenericContainer("sebastianharko/postgres104:latest")
    container.waitingFor(Wait.forLogMessage(".*ready to accept connections.*\\n", 1))
    container.addExposedPort(5432)
    container.start()
    container
  }

  val testConfig = ConfigFactory.parseString(
    s"""|container.host = "${postgreSQLContainer.getContainerIpAddress}"
        |container.port = ${postgreSQLContainer.getMappedPort(5432)}
        |postgres.host = $${container.host}
        |postgres.port = $${container.port}
        |postgres-starved.host = $${container.host}
        |postgres-starved.port = $${container.port}""".stripMargin,
    ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
  ).resolve()


  val regularConfig = ConfigFactory.load

  val mergedConfig = testConfig.withFallback(regularConfig)

  implicit val system = ActorSystem("flixdb", config = mergedConfig)

  val postgreSQL: PostgreSQLExtensionImpl = PostgreSQL(system)

  val postgreSQLStarvedConnectionPool: PostgreSQLExtensionImpl =
    new PostgreSQLExtensionImpl(system) {
      override def poolName = "postgres-starved"
    }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    postgreSQLContainer.stop()
  }

  test("Connecting to PostgreSQL") {
    postgreSQL.validate().futureValue shouldBe Done
  }

  test("Creating the schema") {
    postgreSQL
      .createTablesIfNotExists(prefix = "megacorp")
      .futureValue shouldBe Done
  }

  val event1EntityId = s"account|${randomUUID().toString}"

  val event1: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = event1EntityId,
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "John Smith"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L
  )

  val event2: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"account|${randomUUID().toString}",
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "Rachel Smith"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L
  )

  val event3: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"account|${randomUUID().toString}",
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "Daniel Smith"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L
  )

  test("Appending an event to the events table") {
    postgreSQL
      .appendEvents(
        "megacorp",
        List(event1)
      )
      .futureValue shouldBe Done
  }

  test("Appending the same event again to the events table") {

    val result = postgreSQL
      .appendEvents(
        "megacorp",
        List(event1)
      )
      .failed
      .futureValue

    result shouldBe an[SQLCompositeException]
    result
      .asInstanceOf[SQLCompositeException]
      .isUniqueConstraintViolation shouldBe true

  }

  test("Appending a batch of events to the events table") {
    postgreSQL
      .appendEvents(
        "megacorp",
        List(event2, event3)
      )
      .futureValue shouldBe Done
  }

  test("Appending the same batch of events again to the events table") {
    val result = postgreSQL
      .appendEvents(
        "megacorp",
        List(event2, event3)
      )
      .failed
      .futureValue

    result shouldBe an[SQLCompositeException]
    result
      .asInstanceOf[SQLCompositeException]
      .isUniqueConstraintViolation shouldBe true
  }

  test("Appending an event to the events table with the wrong prefix") {
    val result =
      postgreSQL.appendEvents("smallcorp", List(event1)).failed.futureValue
    result shouldBe an[SQLCompositeException]
    result.asInstanceOf[SQLCompositeException].isUndefinedTable shouldBe true
  }

  test("Getting events for an entity") {
    val result =
      postgreSQL.selectEvents("megacorp", stream = "accounts", event1EntityId).futureValue
    result shouldBe an[List[EventEnvelope]]

    val fetchedEvent = result.asInstanceOf[List[EventEnvelope]].head

    fetchedEvent.eventId shouldBe event1.eventId
    fetchedEvent.subStreamId shouldBe event1.subStreamId
    fetchedEvent.eventType shouldBe event1.eventType
    fetchedEvent.sequenceNum shouldBe event1.sequenceNum
    fetchedEvent.timestamp should not be -1
    fetchedEvent.data shouldBe event1.data
    fetchedEvent.stream shouldBe event1.stream
    fetchedEvent.tags shouldBe event1.tags
  }

  test("No available connections in the pool") {

    // get the one and only connection
    // available
    postgreSQLStarvedConnectionPool.ds.getConnection

    val f = postgreSQLStarvedConnectionPool
      .appendEvents("megacorp", List(event1))
      .failed

    import scala.concurrent.duration._
    whenReady(f, timeout = Timeout(300 milliseconds)) { result: Throwable => result shouldBe an[SQLCompositeException] }

  }

  test("We can close connection pools") {
    postgreSQL.closePools().futureValue shouldBe Done
    postgreSQLStarvedConnectionPool.closePools().futureValue shouldBe Done
  }

}
