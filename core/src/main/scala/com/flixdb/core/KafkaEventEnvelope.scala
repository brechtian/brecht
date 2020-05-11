package com.flixdb.core

import spray.json.DefaultJsonProtocol

/**
  * @param eventId UUID for this event
  * @param subStreamId Id of the domain entity (e.g., account-123)
  * @param eventType FQN of the event class
  * @param sequenceNum Sequence number of the event (per entity)
  * @param data JSON data (actual event payload)
  * @param stream Name of the stream that the event belongs to (e.g., account)
  * @param tags Other streams that the event belongs to
  * @param timestamp the number of milliseconds since January 1, 1970, 00:00:00 GMT
  * @param snapshot boolean that indicates if this envelope contains a snapshot event
  */
final case class KafkaEventEnvelope(
    eventId: String,
    subStreamId: String,
    eventType: String,
    sequenceNum: Int,
    data: String,
    stream: String,
    tags: List[String],
    timestamp: Long,
    snapshot: Boolean
)

object KafkaEventEnvelope extends DefaultJsonProtocol {

  import spray.json.{JsArray, JsNumber, JsObject, JsString, _}

  implicit object KafkaEventEnvelopeJsonFormat extends RootJsonFormat[KafkaEventEnvelope] {
    override def write(ee: KafkaEventEnvelope): JsValue =
      JsObject(
        "eventId" -> JsString(ee.eventId),
        "subStreamId" -> JsString(ee.subStreamId),
        "eventType" -> JsString(ee.eventType),
        "sequenceNum" -> JsNumber(ee.sequenceNum),
        "data" -> ee.data.parseJson,
        "stream" -> JsString(ee.stream),
        "tags" -> JsArray(ee.tags.map(t => JsString(t)).toVector),
        "timestamp" -> JsNumber(ee.timestamp),
        "snapshot" -> JsBoolean(ee.snapshot)
      )

    override def read(json: JsValue): KafkaEventEnvelope = {
      val fields =
        List("eventId", "subStreamId", "eventType", "sequenceNum", "data", "stream", "tags", "timestamp", "snapshot")
      json.asJsObject.getFields(fields: _*) match {
        case Seq(
            JsString(eventId),
            JsString(subStreamId),
            JsString(eventType),
            JsNumber(sequenceNum),
            data: JsValue,
            JsString(stream),
            JsArray(tagsJsArray),
            JsNumber(timestamp),
            JsBoolean(snapshot)
            ) =>
          KafkaEventEnvelope(
            eventId,
            subStreamId,
            eventType,
            sequenceNum.toInt,
            data.compactPrint,
            stream,
            tagsJsArray.collect { case JsString(tag) => tag }.toList,
            timestamp.toInt,
            snapshot
          )
        case _ => deserializationError(s"Invalid JSON: required fields are ${fields.mkString(", ")}")
      }
    }
  }

}
