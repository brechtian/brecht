package com.flixdb.core

import java.util.UUID.randomUUID

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
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

  test("We can append some events") {
    postgreSQL.createTablesIfNotExists("megacorp")
    postgreSQL.appendEvents("megacorp", List(event1, event2, event3)).futureValue shouldBe Done
    postgreSQL.appendEvents("megacorp", List(event4)).futureValue shouldBe Done
    postgreSQL.appendEvents("megacorp", List(event5)).futureValue shouldBe Done
    postgreSQL.selectEvents("megacorp", "accounts", "account-0").futureValue.size shouldBe 3
  }

  test("We can save a snapshot") {
    postgreSQL.saveSnapshot("megacorp", snapshot).futureValue shouldBe Done
    val selectEvents = postgreSQL.selectEvents("megacorp", "accounts", "account-0").futureValue
    selectEvents.size shouldBe 1
    selectEvents.head shouldBe snapshot
  }

  test("The snapshot that we took did not affect other streams or sub streams") {

    val account1Events = postgreSQL.selectEvents("megacorp", "accounts", "account-1").futureValue
    account1Events.size shouldBe 1
    account1Events.head shouldBe event4

    val user0Events =
      postgreSQL.selectEvents("megacorp", "users", "user-0").futureValue
    user0Events.size shouldBe 1
    user0Events.head shouldBe event5
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
          |postgres.host = $${container.host}
          |postgres.port = $${container.port}""".stripMargin,
      ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
    )
    .resolve()

  val regularConfig = ConfigFactory.load

  val mergedConfig = testConfig.withFallback(regularConfig)

  val system = ActorSystem("flixdb", config = mergedConfig)

  val postgreSQL: PostgreSQLExtensionImpl = PostgreSQL(system)

  override def afterAll: Unit = {
    import scala.concurrent.duration._
    postgreSQL.closePools()
    TestKit.shutdownActorSystem(system, duration = 30.seconds, verifySystemShutdown = true)
    postgreSQLContainer.stop()
  }

}