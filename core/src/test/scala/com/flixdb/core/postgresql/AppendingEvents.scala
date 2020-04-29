package com.flixdb.core.postgresql

import java.util.UUID.randomUUID

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.flixdb.core.EventEnvelope
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.Wait

abstract class AppendingEventsSpec extends BaseAppendingEventsSpec {

  val event1: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = "account-0",
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "Michael Jackson"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  val event2: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"account-1",
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "Madonna"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  val event3: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"account-2",
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "George Michael"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  test("Connecting to PostgreSQL") {
    dataAccess.validate().futureValue shouldBe Done
  }

  test("Creating required tables") {
      dataAccess
        .createTablesIfNotExists(prefix = "megacorp")
        .futureValue shouldBe Done
  }

  test("Appending an event to the events table") {
    dataAccess
      .appendEvents(
        "megacorp",
        List(event1)
      )
      .futureValue shouldBe Done
  }

  test("Appending the same event again to the events table") {

    val result = dataAccess
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
    dataAccess
      .appendEvents(
        "megacorp",
        List(event2, event3)
      )
      .futureValue shouldBe Done
  }

  test("Appending the same batch of events again to the events table") {
    val result = dataAccess
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
      dataAccess.appendEvents("smallcorp", List(event1)).failed.futureValue
    result shouldBe an[SQLCompositeException]
    result.asInstanceOf[SQLCompositeException].isUndefinedTable shouldBe true
  }

  test("Getting events for an entity") {
    val result =
      dataAccess.selectEvents("megacorp", stream = "accounts", event1.subStreamId).futureValue
    result shouldBe an[List[EventEnvelope]]

    val fetchedEvent = result.asInstanceOf[List[EventEnvelope]].head

    fetchedEvent shouldBe event1

  }

  test("Gettings events for unknown entity") {
    val result =
      dataAccess.selectEvents("megacorp", stream = "accounts", "account-7").futureValue

    result shouldBe an[List[EventEnvelope]]

    result shouldBe Nil

  }

}

class AppendingEventsSpecWithPostgreSQL104 extends AppendingEventsSpec {
  override def imageName = "sebastianharko/postgres104:latest"
}

class AppendingEventsSpecWithPostgreSQL96 extends AppendingEventsSpec {
  override def imageName = "sebastianharko/postgres96:latest"
}

class AppendingEventsSpecWithPostgreSQL95 extends AppendingEventsSpec {
  override def imageName = "sebastianharko/postgres95:latest"
}

abstract class BaseAppendingEventsSpec
    extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with Matchers {

  val log = LoggerFactory.getLogger(classOf[BaseAppendingEventsSpec])

  def imageName: String

  implicit val defaultPatienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(10, Seconds)),
      interval = scaled(Span(2, Seconds))
    )

  val postgreSQLContainer: GenericContainer[_] = {
    val container =
      new GenericContainer(
        imageName
      )
    container.waitingFor(Wait.forLogMessage(".*ready to accept connections.*\\n", 1))
    container.addExposedPort(5432)
    container.start()
    container
  }

  val testConfig = ConfigFactory
    .parseString(
      s"""|container.host = "${postgreSQLContainer.getContainerIpAddress}"
          |container.port = ${postgreSQLContainer.getMappedPort(5432)}
          |postgresql-main-pool.host = $${container.host}
          |postgresql-main-pool.port = $${container.port}""".stripMargin,
      ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
    )
    .resolve()

  val regularConfig = ConfigFactory.load

  val mergedConfig = testConfig.withFallback(regularConfig)

  implicit val system = ActorSystem("flixdb", config = mergedConfig)

  val dataAccess = new PostgresSQLDataAccessLayer()

  override def afterAll: Unit = {
    import scala.concurrent.duration._
    dataAccess.closePools()
    TestKit.shutdownActorSystem(system, duration = 30.seconds, verifySystemShutdown = true)
    postgreSQLContainer.stop()
  }

}
