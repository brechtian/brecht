package com.brecht.core

import com.brecht.core.KafkaEventEnvelope._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import spray.json._

class KafkaEventEnvelopeJsonSpec extends AnyFunSuiteLike with BeforeAndAfterAll with Matchers {

  test("Serializing a 'KafkaEventEnvelope' into json") {
    val e1 = KafkaEventEnvelope(
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

  test("Deserializing json into 'KafkaEventEnvelope'") {
    val jsonString = """{
                 |  "data": {
                 |    "owner": "John Smith"
                 |  },
                 |  "eventId": "1af2948a-d4dd-48b0-8ca0-cb0fe7562b3d",
                 |  "eventType": "com.megacorp.AccountCreated",
                 |  "sequenceNum": 1,
                 |  "snapshot": false,
                 |  "stream": "accounts",
                 |  "subStreamId": "account-123",
                 |  "tags": ["megacorp"],
                 |  "timestamp": 42
                 |}
                 |""".stripMargin

    val kafkaEventEnvelope = jsonString.parseJson.convertTo[KafkaEventEnvelope]

    kafkaEventEnvelope.data.parseJson shouldBe JsObject(Map("owner" -> JsString("John Smith")))
    kafkaEventEnvelope.eventId shouldBe "1af2948a-d4dd-48b0-8ca0-cb0fe7562b3d"
    kafkaEventEnvelope.eventType shouldBe "com.megacorp.AccountCreated"
    kafkaEventEnvelope.sequenceNum shouldBe 1
    kafkaEventEnvelope.snapshot shouldBe false
    kafkaEventEnvelope.stream shouldBe "accounts"
    kafkaEventEnvelope.subStreamId shouldBe "account-123"
    kafkaEventEnvelope.tags shouldBe List("megacorp")
    kafkaEventEnvelope.timestamp shouldBe 42L

  }

}
