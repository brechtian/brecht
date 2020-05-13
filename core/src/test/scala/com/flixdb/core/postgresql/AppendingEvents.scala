package com.flixdb.core.postgresql

import java.util.UUID.randomUUID

import akka.Done
import akka.actor.ActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.testkit.TestKit
import com.flixdb.core.EventEnvelope
import com.flixdb.core.postgresql.PostgreSQLActor._
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.Wait

import scala.concurrent.duration._
import scala.util.{Success, Try}

class AppendingEventsSpecWithPostgreSQL1012 extends AppendingEventsSpec {
  override def imageName = "flixdb-docker-images.bintray.io/flixdb/postgresql:10.12"
}

class AppendingEventsSpecWithPostgreSQL117 extends AppendingEventsSpec {
  override def imageName = "flixdb-docker-images.bintray.io/flixdb/postgresql:11.7"
}

class AppendingEventsSpecWithPostgreSQL122 extends AppendingEventsSpec {
  override def imageName = "flixdb-docker-images.bintray.io/flixdb/postgresql:12.2"
}

abstract class AppendingEventsSpec
    extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with Matchers {

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

  val log = LoggerFactory.getLogger(classOf[AppendingEventsSpec])

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
    container.waitingFor(Wait.forLogMessage(".*ready to accept connections.*\\n", 2))
    container.addExposedPort(5432)
    container.start()
    container
  }

  implicit val system = ActorSystem(
    "flixdb",
    config = ConfigFactory
      .parseString(
        s"""|container.host = "${postgreSQLContainer.getContainerIpAddress}"
            |container.port = ${postgreSQLContainer.getMappedPort(5432)}
            |postgresql-main-pool.host = $${container.host}
            |postgresql-main-pool.port = $${container.port}""".stripMargin,
        ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
      )
      .resolve()
      .withFallback(ConfigFactory.load)
  )

  implicit val actorSystem = system.toTyped

  val dataAccess = new PostgresSQLDataAccess()

  val postgreSQLActor: ActorRef[PostgreSQLActor.Request] = PostgreSQLActor.spawn(dataAccess)

  implicit val timeout = akka.util.Timeout(3.seconds)

  implicit val ec = system.toTyped.executionContext

  test("Connecting to PostgreSQL") {
    dataAccess.validate() shouldBe Done
  }

  test("Creating required tables") {
    postgreSQLActor
      .ask[CreateTablesResult](ref => CreateTablesIfNotExists("megacorp", ref))
      .futureValue shouldBe CreateTablesResult(Success(Done))
  }

  test("Appending an event to the events table") {
    postgreSQLActor
      .ask[PublishEventsResult](ref => PublishEventsRequest("megacorp", List(event1), ref))
      .futureValue shouldBe PublishEventsResult(Success(Done))
  }

  test("Appending the same event again to the events table") {
    val result: Try[Done] = postgreSQLActor
      .ask[PublishEventsResult](ref => PublishEventsRequest("megacorp", List(event1), ref))
      .futureValue
      .result

    result.isFailure shouldBe true
    result.failed.get.asInstanceOf[SQLCompositeException].isUniqueConstraintViolation shouldBe true

  }

  test("Appending a batch of events to the events table") {
    val result: Try[Done] = postgreSQLActor
      .ask[PublishEventsResult](ref => PublishEventsRequest("megacorp", List(event2, event3), ref))
      .futureValue
      .result

    result shouldBe Success(Done)
  }

  test("Appending the same batch of events again to the events table") {
    val result: Try[Done] = postgreSQLActor
      .ask[PublishEventsResult](ref => PublishEventsRequest("megacorp", List(event2, event3), ref))
      .futureValue
      .result

    result.isFailure shouldBe true
    result.failed.get.asInstanceOf[SQLCompositeException].isUniqueConstraintViolation shouldBe true
  }

  test("Appending an event to the events table with the wrong prefix") {
    val result: Try[Done] = postgreSQLActor
      .ask[PublishEventsResult](ref => PublishEventsRequest("smallcorp", List(event1), ref))
      .futureValue
      .result

    result.isFailure shouldBe true
    result.failed.get shouldBe an[SQLCompositeException]
    result.failed.get.asInstanceOf[SQLCompositeException].isUndefinedTable shouldBe true
  }

  test("Getting events for an entity") {
    val result: GetEventsResult = postgreSQLActor
      .ask[GetEventsResult](ref => GetEventsRequest("megacorp", event1.stream, event1.subStreamId, ref))
      .futureValue

    result.eventEnvelopes.isSuccess shouldBe true
    val fetchedEvent = result.eventEnvelopes.get.head
    fetchedEvent shouldBe event1

  }

  test("Getting events for unknown entity") {
    val result: GetEventsResult = postgreSQLActor
      .ask[GetEventsResult](ref => GetEventsRequest("megacorp", "accounts", "account-7", ref))
      .futureValue

    result.eventEnvelopes.isSuccess shouldBe true
    result.eventEnvelopes.get shouldBe an[List[EventEnvelope]]
    result.eventEnvelopes.get shouldBe Nil

  }

  override def afterAll: Unit = {
    import scala.concurrent.duration._
    dataAccess.closePools()
    TestKit.shutdownActorSystem(system, duration = 30.seconds, verifySystemShutdown = true)
    postgreSQLContainer.stop()
  }

}
