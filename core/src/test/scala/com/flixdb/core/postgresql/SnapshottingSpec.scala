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

abstract class SnapshottingSpec extends BaseSnapshottingSpec {

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

  test("Appending some events") {
    dataAccess.createTablesIfNotExists("megacorp")
    dataAccess.createTablesIfNotExists("megacorp_backup")
    dataAccess.appendEvents("megacorp", List(event1, event2, event3)).futureValue shouldBe Done
    dataAccess.appendEvents("megacorp", List(event4)).futureValue shouldBe Done
    dataAccess.appendEvents("megacorp", List(event5)).futureValue shouldBe Done
    dataAccess.appendEvents("megacorp_backup", List(event6, event7, event8)).futureValue shouldBe Done
    dataAccess.selectEvents("megacorp", "accounts", "account-0").futureValue.size shouldBe 3
  }

  test("Taking a snapshot") {
    dataAccess.snapshot("megacorp", snapshot).futureValue shouldBe Done
    val selectEvents = dataAccess.selectEvents("megacorp", "accounts", "account-0").futureValue
    selectEvents.size shouldBe 1
    selectEvents.head shouldBe snapshot
  }

  test("Checking that the snapshot we took did not affect other streams or sub streams") {

    val account1Events = dataAccess.selectEvents("megacorp", "accounts", "account-1").futureValue
    account1Events.size shouldBe 1
    account1Events.head shouldBe event4

    val user0Events =
      dataAccess.selectEvents("megacorp", "users", "user-0").futureValue
    user0Events.size shouldBe 1
    user0Events.head shouldBe event5

    val otherNamespaceEvents
      = dataAccess.selectEvents("megacorp_backup", "accounts", "account-0").futureValue

    otherNamespaceEvents.size shouldBe 3
    otherNamespaceEvents shouldBe List(event6, event7, event8)

  }

}

class SnapshottingSpecWithPostgres104 extends SnapshottingSpec {
  override def imageName: String = "sebastianharko/postgres104:latest"
}

class SnapshottingSpecWithPostgres96 extends SnapshottingSpec {
  override def imageName: String = "sebastianharko/postgres96:latest"
}

class SnapshottingSpecWithPostgres95 extends SnapshottingSpec {
  override def imageName: String = "sebastianharko/postgres95:latest"
}


abstract class BaseSnapshottingSpec
  extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with Matchers {

  val log = LoggerFactory.getLogger(classOf[BaseSnapshottingSpec])

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