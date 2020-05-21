package com.brecht.cdc

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{Attributes, KillSwitches, UniqueKillSwitch}
import akka.testkit.{ImplicitSender, TestKit}
import com.brecht.cdc.scaladsl._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, matchers}
import org.testcontainers.containers.GenericContainer

abstract class PostgreSQLCapturerSpec
    extends TestKit(ActorSystem())
    with AnyWordSpecLike
    with ImplicitSender
    with matchers.should.Matchers
    with BeforeAndAfterAll
    with FakeDb
    with Eventually
    with IntegrationPatience {

  val log = Logging(system, classOf[PostgreSQLCapturerSpec])

  implicit val actorSystem = system

  val container: GenericContainer[_]

  def plugin: Plugin

  def userName: String
  def password: String
  def database: String

  lazy val cfg: HikariConfig = {
    val c = new HikariConfig
    c.setDriverClassName(classOf[org.postgresql.Driver].getName)
    val url = s"jdbc:postgresql://${container.getContainerIpAddress}:${container.getMappedPort(5432)}/${database}"
    log.info("JdbcUrl is {}", url)
    c.setJdbcUrl(url)
    c.setUsername(userName)
    c.setPassword(password)
    c.setMaximumPoolSize(2)
    c.setMinimumIdle(0)
    c.setValidationTimeout(250)
    c.setConnectionTimeout(400)
    c
  }

  lazy val fakeDbDataSource: HikariDataSource = new HikariDataSource(cfg)

  lazy val conn = fakeDbDataSource.getConnection() // for FakeDb

  override def beforeAll(): Unit = {
    log.info("Validating HikariCP pool")
    fakeDbDataSource.validate()

    log.info("Creating FakeDb table structure")
    setTimeZoneUtc(conn)
    createCustomersTable(conn)
    createSalesTable(conn)
    createPurchaseOrdersTable(conn)
    createEmployeesTable(conn)
    createImagesTable(conn)
    createWeatherTable(conn)
    createCountriesTable(conn)
  }

  override def afterAll: Unit = {
    log.info("Dropping tables")
    /*
    The following are useful for local testing but not necessary when running this test on proper CI (like Travis) since the CI
    creates fresh docker containers and destroys them after the test is complete anyway.
     */
    dropTableCustomers(conn)
    dropTableSales(conn)
    dropTablePurchaseOrders(conn)
    dropTableEmployees(conn)
    dropTableImages(conn)
    dropTableWeather(conn)
    dropTableCountries(conn)

    log.info("Shutting down connection")
    conn.close()
    log.info("Shutting down HikariCP data source")
    fakeDbDataSource.close()

    log.info("Shutting down actor system")
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
    log.info("Stopping container")
    container.stop()

  }

  "A PostgreSQL change data capture source" must {

    "capture changes to a table with numeric / character / array columns" in {

      truncateCustomers(conn)
      setUpLogicalDecodingSlot(conn, "scalatest_1", plugin.name)

      log.info("inserting data into customers table")
      // some inserts

      insertCustomer(
        conn,
        id = 0,
        fName = "John",
        lName = "Lennon",
        email = "john.lennon@akka.io",
        tags = List("awesome")
      )

      insertCustomer(
        conn,
        id = 1,
        fName = "George",
        lName = "Harrison",
        email = "george.harrison@akka.io",
        tags = List("awesome")
      )

      insertCustomer(
        conn,
        id = 2,
        fName = "Paul",
        lName = "McCartney",
        email = "paul.mccartney@akka.io",
        tags = List("awesome")
      )

      insertCustomer(
        conn,
        id = 3,
        fName = "Ringo",
        lName = "Star",
        email = "ringo.star@akka.io",
        tags = List("awesome")
      )

      // some updates
      updateCustomerEmail(conn, id = 0, "john.lennon@thebeatles.com")
      updateCustomerEmail(conn, id = 1, "george.harrison@thebeatles.com")
      updateCustomerEmail(conn, id = 2, "paul.mccartney@thebeatles.com")
      updateCustomerEmail(conn, id = 3, "ringo.star@thebeatles.com")

      // some deletes
      deleteCustomers(conn)

      val emptyData = Map.empty[String, String]

      val dataSource = new HikariDataSource(cfg)

      ChangeDataCapture(PostgreSQLInstance(dataSource))
        .source(
          PgCdcSourceSettings(
            slotName = "scalatest_1",
            dropSlotOnFinish = true,
            closeDataSourceOnFinish = true,
            plugin = this.plugin
          )
        )
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[ChangeSet])
        .request(9)
        // expecting the insert events first
        .expectNextChainingPF {
          case ChangeSet(_, _, _, RowInserted("public", "customers", _, _, data, _) :: Nil)
              if Map(
                "id" -> "0",
                "first_name" -> "John",
                "last_name" -> "Lennon",
                "email" -> "john.lennon@akka.io",
                "tags" -> "{awesome}"
              ) == data => // success
        }
        .expectNextChainingPF {
          case ChangeSet(_, _, _, RowInserted("public", "customers", _, _, data, _) :: Nil)
              if Map(
                "id" -> "1",
                "first_name" -> "George",
                "last_name" -> "Harrison",
                "email" -> "george.harrison@akka.io",
                "tags" -> "{awesome}"
              ) == data => // success
        }
        .expectNextChainingPF {
          case ChangeSet(_, _, _, RowInserted("public", "customers", _, _, data, _) :: Nil)
              if Map(
                "id" -> "2",
                "first_name" -> "Paul",
                "last_name" -> "McCartney",
                "email" -> "paul.mccartney@akka.io",
                "tags" -> "{awesome}"
              ) == data => // success
        }
        .expectNextChainingPF {
          case ChangeSet(_, _, _, RowInserted("public", "customers", _, _, data, _) :: Nil)
              if Map(
                "id" -> "3",
                "first_name" -> "Ringo",
                "last_name" -> "Star",
                "email" -> "ringo.star@akka.io",
                "tags" -> "{awesome}"
              ) == data => // success
        }
        // expecting the update events next
        .expectNextChainingPF {
          case ChangeSet(_, _, _, RowUpdated("public", "customers", _, _, dataNew, _, _, _) :: Nil)
              if Map(
                "id" -> "0",
                "first_name" -> "John",
                "last_name" -> "Lennon",
                "email" -> "john.lennon@thebeatles.com",
                "tags" -> "{awesome}"
              ) == dataNew => // success
        }
        .expectNextChainingPF {
          case ChangeSet(_, _, _, RowUpdated("public", "customers", _, _, dataNew, _, _, _) :: Nil)
              if Map(
                "id" -> "1",
                "first_name" -> "George",
                "last_name" -> "Harrison",
                "email" -> "george.harrison@thebeatles.com",
                "tags" -> "{awesome}"
              ) == dataNew => // success
        }
        .expectNextChainingPF {
          case ChangeSet(_, _, _, RowUpdated("public", "customers", _, _, dataNew, _, _, _) :: Nil)
              if Map(
                "id" -> "2",
                "first_name" -> "Paul",
                "last_name" -> "McCartney",
                "email" -> "paul.mccartney@thebeatles.com",
                "tags" -> "{awesome}"
              ) == dataNew => // success

        }
        .expectNextChainingPF {
          case ChangeSet(_, _, _, RowUpdated("public", "customers", _, _, dataNew, _, _, _) :: Nil)
              if Map(
                "id" -> "3",
                "first_name" -> "Ringo",
                "last_name" -> "Star",
                "email" -> "ringo.star@thebeatles.com",
                "tags" -> "{awesome}"
              ) == dataNew => // success

        }
        //  expecting the delete events next (all in a single transaction )
        .expectNextChainingPF {
          case ChangeSet(_, _, _, deleteEvents)
              if deleteEvents.size == 4 && deleteEvents.count(_.isInstanceOf[RowDeleted]) == 4 => // success
        }
        .cancel()

      eventually {
        dataSource.isClosed shouldBe true
      }

    }

    "capture changes to a table with jsonb columns" in {

      truncateSales(conn)

      setUpLogicalDecodingSlot(conn, "scalatest_2", plugin.name)

      val dataSource = new HikariDataSource(cfg)

      insertSale(conn, id = 0, info = """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}""")
      updateSale(conn, id = 0, newInfo = """{"name": "alpakka", "countries": ["*"]}""")
      deleteSale(conn, 0)

      import scala.concurrent.duration._

      ChangeDataCapture(PostgreSQLInstance(dataSource))
        .source(
          PgCdcSourceSettings(
            slotName = "scalatest_2",
            closeDataSourceOnFinish = true,
            dropSlotOnFinish = true,
            plugin = this.plugin
          )
        )
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(3)
        .expectNextChainingPF(
          max = 20.seconds, {
            case RowInserted("public", "sales", _, _, data, _)
                if data == Map(
                  "id" -> "0",
                  "info" -> """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}"""
                ) => // success
          }
        )
        .expectNextChainingPF {
          case RowUpdated("public", "sales", _, _, dataNew, _, _, _)
              if dataNew == Map(
                "id" -> "0",
                "info" -> """{"name": "alpakka", "countries": ["*"]}"""
              ) => // success
        }
        .expectNextChainingPF {
          case RowDeleted(_, _, _, _, _, _) => // success
        }
        .cancel()

      eventually {
        dataSource.isClosed shouldBe true
      }

    }

    "capture changes to a table with xml columns" in {

      truncatePurchaseOrders(conn)

      setUpLogicalDecodingSlot(conn, "scalatest_3", plugin.name)

      val dataSource = new HikariDataSource(cfg)

      val xml = // from: https://msdn.microsoft.com/en-us/library/ms256129(v=vs.110).aspx
        """<?xml version="1.0"?>
          |<purchaseOrder xmlns="http://tempuri.org/po.xsd" orderDate="1999-10-20">
          |    <shipTo country="US">
          |        <name>Alice Smith</name>
          |        <street>123 Maple Street</street>
          |        <city>Mill Valley</city>
          |        <state>CA</state>
          |        <zip>90952</zip>
          |    </shipTo>
          |    <billTo country="US">
          |        <name>Robert Smith</name>
          |        <street>8 Oak Avenue</street>
          |        <city>Old Town</city>
          |        <state>PA</state>
          |        <zip>95819</zip>
          |    </billTo>
          |    <comment>Hurry, my lawn is going wild!</comment>
          |    <items>
          |        <item partNum="872-AA">
          |            <productName>Lawnmower</productName>
          |            <quantity>1</quantity>
          |            <USPrice>148.95</USPrice>
          |            <comment>Confirm this is electric</comment>
          |        </item>
          |        <item partNum="926-AA">
          |            <productName>Baby Monitor</productName>
          |            <quantity>1</quantity>
          |            <USPrice>39.98</USPrice>
          |            <shipDate>1999-05-21</shipDate>
          |        </item>
          |    </items>
          |</purchaseOrder>""".stripMargin

      insertPurchaseOrder(conn, 0, xml)

      deletePurchaseOrder(conn, id = 0)

      ChangeDataCapture(PostgreSQLInstance(dataSource))
        .source(
          PgCdcSourceSettings(
            slotName = "scalatest_3",
            dropSlotOnFinish = true,
            closeDataSourceOnFinish = true,
            plugin = this.plugin
          )
        )
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(2)
        .expectNextChainingPF {
          case RowInserted("public", "purchase_orders", _, _, _, _) => // success
        }
        .expectNextChainingPF {
          case RowDeleted(_, _, _, _, _, _) => // success
        }
        .cancel()

      eventually {
        dataSource.isClosed shouldBe true
      }

    }

    "be able to deal with bytea columns" in {

      truncateImages(conn)

      setUpLogicalDecodingSlot(conn, "scalatest_4", plugin.name)

      val dataSource = new HikariDataSource(cfg)

      val expectedByteArray: Array[Byte] = {
        import java.nio.file.{Files, Paths}
        Files.readAllBytes(Paths.get(this.getClass.getResource("/scala-icon.png").toURI))
      }

      insertImage(conn, 0, "/scala-icon.png") // from postgresql-cdc/src/test/resources/scala-icon.png
      deleteImages(conn)

      import javax.xml.bind.DatatypeConverter // this has a parseHexBinary method that turns out to be useful here

      ChangeDataCapture(PostgreSQLInstance(dataSource))
        .source(
          PgCdcSourceSettings(
            slotName = "scalatest_4",
            dropSlotOnFinish = true,
            closeDataSourceOnFinish = true,
            plugin = this.plugin
          )
        )
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(2)
        .expectNextChainingPF {
          case RowInserted("public", "images", _, _, data, _)
              // from PG docs: The "hex" format encodes binary data as 2 hexadecimal digits per byte, most significant nibble first.
              // The entire string is preceded by the sequence \x
              // NOTE: some inconsistency between the Wal2Plugin and the TestDecoding plugin here.
              // I think that the Wal2Json plugin, takes out the \x prefix
              if (DatatypeConverter.parseHexBinary(data("image").substring(2)) sameElements expectedByteArray) || (DatatypeConverter
                .parseHexBinary(data("image")) sameElements expectedByteArray) => // success
        }
        .expectNextChainingPF {
          case RowDeleted("public", "images", _, _, _, _) => // success
        }
        .cancel()

      eventually {
        dataSource.isClosed shouldBe true
      }

    }

    "be able to deal with null columns" in {

      truncateEmployees(conn)

      setUpLogicalDecodingSlot(conn, "scalatest_5", plugin.name)

      val dataSource = new HikariDataSource(cfg)

      insertEmployee(conn, 0, "Giovanni", "employee")
      updateEmployee(conn, 0, null)
      deleteEmployees(conn)

      ChangeDataCapture(PostgreSQLInstance(dataSource))
        .source(
          PgCdcSourceSettings(
            slotName = "scalatest_5",
            dropSlotOnFinish = true,
            closeDataSourceOnFinish = true,
            plugin = this.plugin
          )
        )
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(3)
        .expectNextChainingPF {
          case RowInserted("public", "employees", _, _, _, _) => // success
        }
        .expectNextChainingPF {
          case RowUpdated("public", "employees", _, _, data, _, _, _)
              // NOTE: some inconsistency between plugins: "COLUMN_NAME" vs just COLUMN_NAME
              // but probably not worth the effort to fix this inconsistency
              if data.get("\"position\"").contains("null") || data.get("position").contains("null") => // success
        }
        .expectNextChainingPF {
          case RowDeleted("public", "employees", _, _, _, _) => // success
        }
        .cancel()

      eventually {
        dataSource.isClosed shouldBe true
      }

    }

    "be able to get both old version / new version of a row - in case of an update operation on a table with replica identity set to full" in {

      truncateWeather(conn)

      setUpLogicalDecodingSlot(conn, "scalatest_6", plugin.name)

      val dataSource = new HikariDataSource(cfg)

      insertWeather(conn, 0, "Seattle", "rainy")
      updateWeather(conn, 0, "sunny")
      deleteWeathers(conn)

      ChangeDataCapture(PostgreSQLInstance(dataSource))
        .source(
          PgCdcSourceSettings(
            slotName = "scalatest_6",
            dropSlotOnFinish = true,
            closeDataSourceOnFinish = true,
            plugin = this.plugin
          )
        )
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(3)
        .expectNextChainingPF {
          case RowInserted("public", tableName, _, _, _, _)
              if (tableName == """"WEATHER"""" || tableName == "WEATHER") => // success
          // NOTE: some inconsistency between plugins: "COLUMN_NAME" vs just COLUMN_NAME
          // but probably not worth the effort to fix this inconsistency
        }
        .expectNextChainingPF {
          case RowUpdated("public", _, _, _, dataNew, dataOld, _, _)
              if (dataNew -> dataOld) == (Map("id" -> "0", "city" -> "Seattle", "weather" -> "sunny") ->
                Map("id" -> "0", "city" -> "Seattle", "weather" -> "rainy")) => // success
        }
        .expectNextChainingPF {
          case del: RowDeleted => // success
        }
        .cancel()

      eventually {
        dataSource.isClosed shouldBe true
      }

    }

    "be able to ignore tables and columns" in {

      truncateSales(conn)
      truncateEmployees(conn)

      setUpLogicalDecodingSlot(conn, "scalatest_7", plugin.name)

      val dataSource = new HikariDataSource(cfg)

      // employees (ignored)
      insertEmployee(conn, 0, "Giovanni", "employee")
      updateEmployee(conn, 0, null)
      deleteEmployees(conn)

      // sales (not ignored)
      insertSale(conn, id = 0, info = """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}""")
      updateSale(conn, id = 0, newInfo = """{"name": "alpakka", "countries": ["*"]}""")
      deleteSale(conn, 0)

      ChangeDataCapture(PostgreSQLInstance(dataSource))
        .source(
          PgCdcSourceSettings(
            slotName = "scalatest_7",
            dropSlotOnFinish = true,
            closeDataSourceOnFinish = true,
            plugin = this.plugin
          ).withColumnsToIgnore(Map("employees" -> List("*"), "sales" -> List("info")))
        )
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(3)
        .expectNextChainingPF {
          case RowInserted("public", "sales", _, _, _, _) => // success
        }
        .expectNextChainingPF {
          case RowUpdated("public", "sales", _, _, dataNew, _, _, _) if !dataNew.contains("info") => // success
        }
        .expectNextChainingPF {
          case RowDeleted("public", "sales", _, _, _, _) => // success
        }
        .cancel()

      eventually {
        dataSource.isClosed shouldBe true
      }

    }

    "be able to capture changes from a table with no primary key" in {

      truncateCountries(conn)
      val slotName = "scalatest_8"
      val dataSource = new HikariDataSource(cfg)

      setUpLogicalDecodingSlot(conn, slotName, plugin.name)

      insertCountry(conn, "Canada", "North America")
      insertCountry(conn, "Iceland", "Europe")
      updateCountry(conn, "Iceland", "North America")
      deleteCountry(conn, "Canada")

      val cdc = ChangeDataCapture(PostgreSQLInstance(dataSource))

      val source = cdc.source(
        PgCdcSourceSettings(
          slotName = slotName,
          dropSlotOnFinish = true,
          closeDataSourceOnFinish = true,
          plugin = this.plugin
        )
      )

      source
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[ChangeSet])
        .request(4)
        // it seems like we can capture inserts properly but we are not getting
        // any data for updates and deletes (depending on the plugin)
        .expectNextChainingPF {
          case ChangeSet(_, _, _, List(RowInserted("public", "countries", _, _, _, _))) => // success
        }
        .expectNextChainingPF {
          case ChangeSet(_, _, _, List(RowInserted("public", "countries", _, _, _, _))) => // success
        }
        .expectNextChainingPF {
          case c: ChangeSet => // success
        }
        .expectNextChainingPF {
          case c: ChangeSet => // success
        }
        .cancel()

      eventually {
        dataSource.isClosed shouldBe true
      }
    }

    "be able to work in 'at-least-once' mode" in {

      truncateEmployees(conn)

      val dataSource = new HikariDataSource(cfg)

      val slotName = "scalatest_9"

      setUpLogicalDecodingSlot(conn, slotName, plugin.name)

      (1 to 100).foreach { id: Int => insertEmployee(conn, id, "Giovanni", "employee") }

      val cdc = ChangeDataCapture(PostgreSQLInstance(dataSource))

      var items = List[Change]()

      val cdcSourceSettings = PgCdcSourceSettings(
        slotName = slotName,
        dropSlotOnFinish = true,
        closeDataSourceOnFinish = true,
        plugin = this.plugin,
        mode = Modes.Peek
      )

      val cdcAckFlowSettings = PgCdcAckSettings(slotName)

      val killSwitch: UniqueKillSwitch =
        cdc
          .source(cdcSourceSettings)
          .mapConcat(_.changes)
          .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
          .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
          .map(ch => (ch, AckLogSeqNum(ch.commitLogSeqNum)))
          .via(cdc.ackFlow(cdcAckFlowSettings))
          .wireTap((item: (Change, AckLogSeqNum)) => items = item._1 +: items)
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)
          .run()

      eventually {
        items should have size 100
        items.foreach { change => change shouldBe an[RowInserted] }
      }

      killSwitch.shutdown()

      eventually {
        dataSource.isClosed shouldBe true
      }

    }

  }

}
