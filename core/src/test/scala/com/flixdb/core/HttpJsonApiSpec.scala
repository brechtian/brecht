package com.flixdb.core

import java.util.UUID.randomUUID

import akka.Done
import akka.http.javadsl.model.ContentTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.flixdb.core.postgresql.SQLCompositeException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._

import scala.concurrent.Future

class HttpJsonApiSpec extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {

  val fakeEventStoreController = new EventStoreController {
    override def createNamespace(namespace: String): Future[Done] = {
      Future.successful(Done)
    }

    override def getEvents(namespace: String, stream: String, subStreamId: String): Future[List[EventEnvelope]] = {
      (namespace, stream, subStreamId) match {
        case ("megacorp", "accounts", "account-0") =>
          Future.successful(
            List(
              EventEnvelope(
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
            )
          )
        case ("smallcorp", _, _) =>
          Future.failed(new SQLCompositeException(null, Nil) {
            override def isUndefinedTable: Boolean = true
          })
        case _ => Future.failed(new Exception)
      }
    }

    override def publishEvents(
        namespace: String,
        eventEnvelopes: List[EventEnvelope]
    ): Future[Done] = {
      (namespace, eventEnvelopes) match {
        case ("megacorp", _) => Future.successful(Done)
        case ("smallcorp", _) =>
          Future.failed(new SQLCompositeException(null, Nil) {
            override def isUndefinedTable: Boolean = true
          })
        case _ => Future.failed(new Exception)
      }
    }
  }

  val httpJsonRoutes = new HttpJsonRoutes(fakeEventStoreController)

  "The HTTP/JSON API" should {

    "allow us to create a namespace" in {
      Post("/megacorp") ~> httpJsonRoutes.routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "allow us to get the event log of an entity" in {
      Get("/megacorp/events/accounts/account-0") ~> httpJsonRoutes.routes ~> check {
        status shouldEqual StatusCodes.OK
        val jsonResponse = responseAs[String].parseJson
        jsonResponse shouldBe an[JsArray]
        jsonResponse match {
          case JsArray(Vector(head: JsObject)) =>
            head.fields("data").asJsObject.fields("owner") shouldBe JsString("Michael Jackson")
        }
      }
    }

    "return 404 when we try to get the event log of an entity in a namespace that doesn't exist" in {
      Get("/smallcorp/events/accounts/account-0") ~> httpJsonRoutes.routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "allows us to publish some events" in {
      val json1 = """|{
                     |  "data":{"owner":"John Smith"},
                     |  "eventType":"com.megacorp.AccountCreated",
                     |  "eventId":"1af2948a-d4dd-48b0-8ca0-cb0fe7562b3d",
                     |  "sequenceNum":1,
                     |  "tags":["megacorp"]
                     |}""".stripMargin

      val json = s"[$json1]"

      Post("/megacorp/events/accounts/account-0")
        .withEntity(ContentTypes.APPLICATION_JSON, json) ~> httpJsonRoutes.routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "return 404 when we try to publish some events in a namespace that doesn't exist" in {
      val json1 = """|{
                     |  "data":{"owner":"John Smith"},
                     |  "eventType":"com.megacorp.AccountCreated",
                     |  "eventId":"1af2948a-d4dd-48b0-8ca0-cb0fe7562b3d",
                     |  "sequenceNum":1,
                     |  "tags":["megacorp"]
                     |}""".stripMargin

      val json = s"[$json1]"

      Post("/smallcorp/events/accounts/account-0")
        .withEntity(ContentTypes.APPLICATION_JSON, json) ~> httpJsonRoutes.routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

  }

}
