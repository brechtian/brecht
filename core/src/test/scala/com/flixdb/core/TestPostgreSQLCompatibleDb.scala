package com.flixdb.core

import java.util.UUID._

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import scala.language.postfixOps

class TestPostgreSQLCompatibleDb
    extends TestKit(ActorSystem("flixdb"))
    with AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with Matchers {

  implicit val defaultPatienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(10, Seconds)),
      interval = scaled(Span(2, Seconds))
    )

  val postgreSQL: PostgreSQLExtensionImpl = PostgreSQL(system)

  val postgreSQLStarvedConnectionPool: PostgreSQLExtensionImpl =
    new PostgreSQLExtensionImpl(system) {
      override def poolName = "postgres-starved"
    }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
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
    entityId = event1EntityId,
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "John Smith"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L
  )

  val event2: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    entityId = s"account|${randomUUID().toString}",
    eventType = "com.megacorp.AccountCreated",
    sequenceNum = 0,
    data = """{"owner": "Rachel Smith"}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L
  )

  val event3: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    entityId = s"account|${randomUUID().toString}",
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
    fetchedEvent.entityId shouldBe event1.entityId
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
