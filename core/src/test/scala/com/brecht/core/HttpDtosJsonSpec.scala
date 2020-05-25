package com.brecht.core

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class HttpDtosJsonSpec extends JsonSupport with AnyFunSuiteLike with BeforeAndAfterAll with Matchers {

  import spray.json._

  test("Serializing an 'Event' into json") {

    val e1 = Dtos.Event(
      eventId = "1af2948a-d4dd-48b0-8ca0-cb0fe7562b3d",
      eventType = "com.megacorp.AccountCreated",
      subStreamId = "account-123",
      sequenceNum = 1,
      data = """{"owner":"John Smith"}""",
      stream = "accounts",
      tags = List("megacorp"),
      timestamp = 42L,
      snapshot = false
    )

    val json = e1.toJson.asJsObject()

    json.getFields("eventId") shouldBe Seq(JsString(e1.eventId))
    json.getFields("eventType") shouldBe Seq(JsString(e1.eventType))
    json.getFields("subStreamId") shouldBe Seq(JsString(e1.subStreamId))
    json.getFields("stream") shouldBe Seq(JsString(e1.stream))
    json.getFields("sequenceNum") shouldBe Seq(JsNumber(e1.sequenceNum))
    json.getFields("tags") shouldBe Seq(JsArray(JsString("megacorp")))
    json.getFields("timestamp") shouldBe Seq(JsNumber(42))
    json.getFields("data") shouldBe an[Seq[JsObject]]
    json.getFields("data").head.asJsObject.getFields("owner") shouldBe Seq(JsString("John Smith"))
    json.getFields("snapshot").head shouldBe JsBoolean(false)
  }

  test("Deserializing json into 'PostEvent'") {

    val json: JsValue =
      """|{"data":{"owner":"John Smith"},
         |"eventType":"com.megacorp.AccountCreated",
         |"eventId":"1af2948a-d4dd-48b0-8ca0-cb0fe7562b3d",
         |"sequenceNum":1,
         |"tags":["megacorp"]
         |}""".stripMargin.parseJson

    val data: JsObject = json.asJsObject.fields("data").asJsObject
    data.fields("owner") shouldBe JsString("John Smith")

    val e = json.convertTo[Dtos.PostEvent]

    e.eventId shouldBe "1af2948a-d4dd-48b0-8ca0-cb0fe7562b3d"
    e.eventType shouldBe "com.megacorp.AccountCreated"
    e.sequenceNum shouldBe Some(1)
    e.data shouldBe """{"owner":"John Smith"}"""
    e.tags shouldBe Option(List("megacorp"))
  }

}
