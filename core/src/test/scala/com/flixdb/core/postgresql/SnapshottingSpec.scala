package com.flixdb.core.postgresql

import java.util.UUID.randomUUID

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, DispatcherSelector}
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

class SnapshottingSpecWithPostgres1012 extends SnapshottingSpec {
  override def imageName: String =
    "flixdb-docker-images.bintray.io/flixdb/postgresql:10.12"
}

class SnapshottingSpecWithPostgres117 extends SnapshottingSpec {
  override def imageName: String =
    "flixdb-docker-images.bintray.io/flixdb/postgresql:11.7"
}

class SnapshottingSpecWithPostgres122 extends SnapshottingSpec {
  override def imageName: String =
    "flixdb-docker-images.bintray.io/flixdb/postgresql:12.2"

}

abstract class SnapshottingSpec
    extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with Matchers {

  val log = LoggerFactory.getLogger(classOf[SnapshottingSpec])

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

  implicit val typedSystem = system.toTyped

  val dataAccess = new PostgresSQLDataAccess()

  val postgreSQLActor: ActorRef[PostgreSQLActor.Request] = {
    val postgreSQLActor: ActorRef[PostgreSQLActor.Request] = {
      system.spawn(
        behavior = PostgreSQLActor.apply(dataAccess),
        name = s"postgresql",
        DispatcherSelector.fromConfig("blocking-io-dispatcher")
      )
    }
    postgreSQLActor
  }

  implicit val timeout = akka.util.Timeout(3.seconds)

  implicit val ec = system.toTyped.executionContext

  override def afterAll: Unit = {
    import scala.concurrent.duration._
    dataAccess.closePools()
    TestKit.shutdownActorSystem(system, duration = 30.seconds, verifySystemShutdown = true)
    postgreSQLContainer.stop()
  }

  val event1: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = "account-0",
    eventType = "com.megacorp.MoneyDeposited",
    sequenceNum = 0,
    data = """{"owner": "Michael Jackson", "amount": 20}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  val event2: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = "account-0",
    eventType = "com.megacorp.MoneyDeposited",
    sequenceNum = 1,
    data = """{"owner": "Michael Jackson", "amount": 30}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 43L,
    snapshot = false
  )

  val event3: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"account-0",
    eventType = "com.megacorp.MoneyDeposited",
    sequenceNum = 2,
    data = """{"owner": "Michael Jackson", "amount": 15}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  val event4: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"account-1",
    eventType = "com.megacorp.MoneyDeposited",
    sequenceNum = 0,
    data = """{"owner": "Somebody Else", "amount": 10}""",
    stream = "accounts",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  val event5: EventEnvelope = EventEnvelope(
    eventId = randomUUID().toString,
    subStreamId = s"user-0",
    eventType = "com.megacorp.UserCreated",
    sequenceNum = 0,
    data = """{"userName": "MichaelJackson93"}""",
    stream = "users",
    tags = List("megacorp"),
    timestamp = 42L,
    snapshot = false
  )

  val event6 = event1 // same as event1 but we'll write in a different namespace

  val event7 = event2 // same as event2 but we'll write in a different namespace

  val event8 = event3 // same as event3 but we'll write in a different namespace

  test("Appending some events") {
    postgreSQLActor
      .ask[CreateTablesResult](ref => CreateTablesIfNotExists("megacorp", ref))
      .futureValue
      .result
      .isSuccess shouldBe true

    postgreSQLActor
      .ask[CreateTablesResult](ref => CreateTablesIfNotExists("megacorp_backup", ref))
      .futureValue
      .result
      .isSuccess shouldBe true

    postgreSQLActor
      .ask[PublishEventsResult](ref =>
        PublishEventsRequest("megacorp", List(event1, event2, event3, event4, event5), ref)
      )
      .futureValue
      .result
      .isSuccess shouldBe true

    postgreSQLActor
      .ask[PublishEventsResult](ref => PublishEventsRequest("megacorp_backup", List(event6, event7, event8), ref))
      .futureValue
      .result
      .isSuccess shouldBe true

  }

  test("Taking a snapshot") {

    val snapshot: EventEnvelope = EventEnvelope(
      eventId = randomUUID().toString,
      subStreamId = s"account-0",
      eventType = "com.megacorp.AccountBalance",
      sequenceNum = 3,
      data = """{"owner": "Michael Jackson", "amount": 65}""",
      stream = "accounts",
      tags = List("megacorp"),
      timestamp = 42L,
      snapshot = true
    )

    postgreSQLActor
      .ask[SnapshotResult](ref => SnapshotRequest("megacorp", snapshot, ref))
      .futureValue
      .result
      .isSuccess shouldBe true

    val result =
      postgreSQLActor
        .ask[GetEventsResult](ref => GetEventsRequest("megacorp", "accounts", "account-0", ref))
        .futureValue
        .eventEnvelopes

    result.isSuccess shouldBe true
    val events = result.get
    events.size shouldBe 1
    events.head shouldBe snapshot

  }

  test("Checking that the snapshot we took did not affect other streams or sub streams") {

    val account1Result =
      postgreSQLActor
        .ask[GetEventsResult](ref => GetEventsRequest("megacorp", "accounts", "account-1", ref))
        .futureValue
        .eventEnvelopes

    account1Result.isSuccess shouldBe true
    val account1Events = account1Result.get
    account1Events.size shouldBe 1
    account1Events.head shouldBe event4

    val user0Result =
      postgreSQLActor
        .ask[GetEventsResult](ref => GetEventsRequest("megacorp", "users", "user-0", ref))
        .futureValue
        .eventEnvelopes

    user0Result.isSuccess shouldBe true

    val user0Events = user0Result.get
    user0Events.size shouldBe 1
    user0Events.head shouldBe event5

    val otherNamespaceResult =
      postgreSQLActor
        .ask[GetEventsResult](ref => GetEventsRequest("megacorp_backup", "accounts", "account-0", ref))
        .futureValue
        .eventEnvelopes

    otherNamespaceResult.isSuccess shouldBe true

    val otherNamespaceEvents = otherNamespaceResult.get
    otherNamespaceEvents.size shouldBe 3
    otherNamespaceEvents shouldBe List(event6, event7, event8)

  }

}
