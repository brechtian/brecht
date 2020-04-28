package com.flixdb.cdc

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.Attributes
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, matchers}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.Wait

abstract class PostgreSQLCapturerSpec
    extends TestKit(ActorSystem())
    with AnyWordSpecLike
    with ImplicitSender
    with matchers.should.Matchers
    with BeforeAndAfterAll
    with FakeDb {

  val log = Logging(system, classOf[PostgreSQLCapturerSpec])

  implicit val actorSystem = system

  val container: GenericContainer[_]

  lazy val cfg: HikariConfig = {
    val c = new HikariConfig
    c.setDriverClassName(classOf[org.postgresql.Driver].getName)
    val url = s"jdbc:postgresql://${container.getContainerIpAddress}:${container.getMappedPort(5432)}/postgres"
    log.info("JdbcUrl is {}", url)
    c.setJdbcUrl(url)
    c.setUsername("pguser")
    c.setPassword("pguser")
    c.setMaximumPoolSize(2)
    c.setMinimumIdle(0)
    c.setPoolName("pg")
    c
  }

  lazy val ds: HikariDataSource = new HikariDataSource(cfg)

  lazy val conn = ds.getConnection() // for FakeDb

  override def beforeAll(): Unit = {
    log.info("Validating HikariCP pool")
    ds.validate()

    log.info("Setting up logical decoding slot and creating customers table")
    setTimeZoneUtc(conn)
    setUpLogicalDecodingSlot(conn, "scalatest")
    createCustomersTable(conn)
    createSalesTable(conn)
    createPurchaseOrdersTable(conn)
    createEmployeesTable(conn)
    createImagesTable(conn)
    createWeatherTable(conn)

  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)

    log.info("Dropping logical decoding slot and dropping tables")
    /*
    The following are useful for local testing but not necessary when running this test on proper CI (like Travis) since the CI
    creates fresh docker containers and destroys them after the test is complete anyway.
     */
    dropLogicalDecodingSlot(conn, "scalatest")
    dropTableCustomers(conn)
    dropTableSales(conn)
    dropTablePurchaseOrders(conn)
    dropTableEmployees(conn)
    dropTableImages(conn)
    dropTableWeather(conn)

    conn.close()
    ds.close()

    log.info("Stopping container")
    container.stop()

  }

  "A PostgreSQL change data capture source" must {

    "capture changes to a table with numeric / character / array columns" in {

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

      ChangeDataCapture
        .source(dataSource = ds, PgCdcSourceSettings().withSlotName("scalatest"))
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[ChangeSet])
        .request(9)
        // expecting the insert events first
        .expectNextChainingPF {
          case c @ ChangeSet(_, _, _, RowInserted("public", "customers", _, _, data, _) :: Nil)
              if Map(
                "id" -> "0",
                "first_name" -> "John",
                "last_name" -> "Lennon",
                "email" -> "john.lennon@akka.io",
                "tags" -> "{awesome}"
              ) == data => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, _, _, RowInserted("public", "customers", _, _, data, _) :: Nil)
              if Map(
                "id" -> "1",
                "first_name" -> "George",
                "last_name" -> "Harrison",
                "email" -> "george.harrison@akka.io",
                "tags" -> "{awesome}"
              ) == data => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, _, _, RowInserted("public", "customers", _, _, data, _) :: Nil)
              if Map(
                "id" -> "2",
                "first_name" -> "Paul",
                "last_name" -> "McCartney",
                "email" -> "paul.mccartney@akka.io",
                "tags" -> "{awesome}"
              ) == data => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, _, _, RowInserted("public", "customers", _, _, data, _) :: Nil)
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
          case c @ ChangeSet(_, _, _, RowUpdated("public", "customers", _, _, dataNew, `emptyData`, _, _) :: Nil)
              if Map(
                "id" -> "0",
                "first_name" -> "John",
                "last_name" -> "Lennon",
                "email" -> "john.lennon@thebeatles.com",
                "tags" -> "{awesome}"
              ) == dataNew => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, _, _, RowUpdated("public", "customers", _, _, dataNew, `emptyData`, _, _) :: Nil)
              if Map(
                "id" -> "1",
                "first_name" -> "George",
                "last_name" -> "Harrison",
                "email" -> "george.harrison@thebeatles.com",
                "tags" -> "{awesome}"
              ) == dataNew => // success
        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, _, _, RowUpdated("public", "customers", _, _, dataNew, `emptyData`, _, _) :: Nil)
              if Map(
                "id" -> "2",
                "first_name" -> "Paul",
                "last_name" -> "McCartney",
                "email" -> "paul.mccartney@thebeatles.com",
                "tags" -> "{awesome}"
              ) == dataNew => // success

        }
        .expectNextChainingPF {
          case c @ ChangeSet(_, _, _, RowUpdated("public", "customers", _, _, dataNew, `emptyData`, _, _) :: Nil)
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
          case c @ ChangeSet(_, _, _, deleteEvents)
              if deleteEvents.size == 4 && deleteEvents.count(_.isInstanceOf[RowDeleted]) == 4 => // success
        }

    }

    "capture changes to a table with jsonb columns" in {
      insertSale(conn, id = 0, info = """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}""")
      updateSale(conn, id = 0, newInfo = """{"name": "alpakka", "countries": ["*"]}""")
      deleteSale(conn, 0)

      ChangeDataCapture
        .source(ds, PgCdcSourceSettings().withSlotName("scalatest"))
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(3)
        .expectNextChainingPF {
          case RowInserted("public", "sales", _, _, data, _)
              if data == Map(
                "id" -> "0",
                "info" -> """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}"""
              ) => // success
        }
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

    }

    "capture changes to a table with xml columns" in {

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

      ChangeDataCapture
        .source(ds, PgCdcSourceSettings().withSlotName("scalatest"))
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

    }

    "be able to deal with bytea columns" in {

      val expectedByteArray: Array[Byte] = {
        import java.nio.file.{Files, Paths}
        Files.readAllBytes(Paths.get(this.getClass.getResource("/scala-icon.png").toURI))
      }

      insertImage(conn, 0, "/scala-icon.png") // from postgresql-cdc/src/test/resources/scala-icon.png
      deleteImages(conn)

      import javax.xml.bind.DatatypeConverter // this has a parseHexBinary method that turns out to be useful here

      ChangeDataCapture
        .source(ds, PgCdcSourceSettings().withSlotName("scalatest"))
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(2)
        .expectNextChainingPF {
          case RowInserted("public", "images", _, _, data, _)
              // from PG docs: The "hex" format encodes binary data as 2 hexadecimal digits per byte, most significant nibble first.
              // The entire string is preceded by the sequence \x
              if DatatypeConverter.parseHexBinary(data("image").substring(2)) sameElements expectedByteArray => // success
        }
        .expectNextChainingPF {
          case RowDeleted("public", "images", _, _, _, _) => // success
        }

    }

    "be able to deal with null columns" in {

      insertEmployee(conn, 0, "Giovanni", "employee")
      updateEmployee(conn, 0, null)
      deleteEmployees(conn)

      ChangeDataCapture
        .source(ds, PgCdcSourceSettings().withSlotName("scalatest"))
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(3)
        .expectNextChainingPF {
          case RowInserted("public", "employees", _, _, _, _) => // success
        }
        .expectNextChainingPF {
          case RowUpdated("public", "employees", _, _, data, _, _, _) if data("\"position\"") == "null" => // success
        }
        .expectNextChainingPF {
          case RowDeleted("public", "employees", _, _, _, _) => // success
        }

    }

    "be able to get both old version / new version of a row - in case of an update operation on a table with replica identity set to full" in {

      insertWeather(conn, 0, "Seattle", "rainy")
      updateWeather(conn, 0, "sunny")
      deleteWeathers(conn)

      ChangeDataCapture
        .source(ds, PgCdcSourceSettings().withSlotName("scalatest"))
        .mapConcat(_.changes)
        .log("postgresqlcdc", cs => s"captured change: ${cs.toString}")
        .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .runWith(TestSink.probe[Change])
        .request(3)
        .expectNextChainingPF {
          case RowInserted("public", """"WEATHER"""", _, _, _, _) => // success
        }
        .expectNextChainingPF {
          case RowUpdated("public", """"WEATHER"""", _, _, dataNew, dataOld, _, _)
              if (dataNew -> dataOld) == (Map("id" -> "0", "city" -> "Seattle", "weather" -> "sunny") ->
                Map("id" -> "0", "city" -> "Seattle", "weather" -> "rainy")) => // success
        }
        .expectNextChainingPF {
          case del: RowDeleted => // success
        }

    }

    "be able to ignore tables and columns" in {

      // employees (ignored)
      insertEmployee(conn, 0, "Giovanni", "employee")
      updateEmployee(conn, 0, null)
      deleteEmployees(conn)

      // sales (not ignored)
      insertSale(conn, id = 0, info = """{"name": "alpaca", "countries": ["Peru", "Bolivia", "Ecuador", "Chile"]}""")
      updateSale(conn, id = 0, newInfo = """{"name": "alpakka", "countries": ["*"]}""")
      deleteSale(conn, 0)

      ChangeDataCapture
        .source(
          ds,
          PgCdcSourceSettings().withSlotName("scalatest")
            .withColumnsToIgnore(Map("employees" -> List("*"), "sales" -> List("info")))
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

    }

  }

}

abstract class PostgreSQLImageName extends PostgreSQLCapturerSpec {
  def imageName: String
  override val container: GenericContainer[_] = {
    val container =
      new GenericContainer(
        imageName
      )
    container.waitingFor(Wait.forLogMessage(".*ready to accept connections.*\\n", 1))
    container.addExposedPort(5432)
    container.start()
    container
  }
}

class PostgreSQL104 extends PostgreSQLImageName {
  override def imageName = "sebastianharko/postgres104:latest"
}

class PostgreSQL96 extends PostgreSQLImageName {
  override def imageName = "sebastianharko/postgres96:latest"
}

class PostgreSQL95 extends PostgreSQLImageName {
  override def imageName = "sebastianharko/postgres95:latest"
}

class PostgreSQL94 extends PostgreSQLImageName {
  override def imageName = "sebastianharko/postgres94:latest"
}

