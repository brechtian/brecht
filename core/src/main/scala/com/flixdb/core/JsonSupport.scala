package com.flixdb.core

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.flixdb.core.Dtos.{Event, EventList, PostEvent, PostEventList}
import spray.json.{DefaultJsonProtocol, JsArray, JsNumber, JsObject, JsString, _}

object Dtos {

  final case class Event(
      eventId: String,
      subStreamId: String,
      eventType: String,
      sequenceNum: Int,
      data: String,
      stream: String,
      tags: List[String],
      timestamp: Long
  )

  final case class PostEvent(
      eventId: String,
      eventType: String,
      sequenceNum: Int,
      data: String,
      tags: Option[List[String]]
  )

  final case class EventList(events: List[Event])

  final case class PostEventList(events: List[PostEvent])

}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit object GetEventJsonWriter extends RootJsonWriter[Event] {
    override def write(ee: Event): JsValue =
      JsObject(
        "eventId" -> JsString(ee.eventId),
        "subStreamId" -> JsString(ee.subStreamId),
        "eventType" -> JsString(ee.eventType),
        "sequenceNum" -> JsNumber(ee.sequenceNum),
        "data" -> ee.data.parseJson,
        "stream" -> JsString(ee.stream),
        "tags" -> JsArray(ee.tags.map(t => JsString(t)).toVector),
        "timestamp" -> JsNumber(ee.timestamp)
      )
  }

  implicit object GetEventListJsonWriter extends RootJsonWriter[EventList] {
    override def write(obj: EventList): JsValue =
      JsArray(obj.events.map(_.toJson).toVector)
  }

  implicit object PostEventJsonReader extends RootJsonReader[PostEvent] {
    override def read(value: JsValue): PostEvent = {
      val jsObject = value.asJsObject
      jsObject.getFields("eventId", "eventType", "sequenceNum", "data") match {
        case Seq(
            JsString(eventUniqueId),
            JsString(eventType),
            JsNumber(sequenceNum),
            data: JsValue
            ) =>
          // optional fields
          val tags = jsObject.fields.get("tags") match {
            case Some(JsArray(items)) => Some(items.collect { case JsString(v) => v }.toList)
            case _                    => None
          }

          PostEvent(eventUniqueId, eventType, sequenceNum.toInt, data.compactPrint, tags)

        case _ =>
          deserializationError("Invalid JSON: you may be missing some fields, see documentation")
      }
    }

  }

  implicit object PostEventListJsonReader extends RootJsonReader[PostEventList] {
    override def read(value: JsValue): PostEventList = {
      value match {
        case JsArray(elements: Vector[JsValue]) =>
          PostEventList(elements.map(_.convertTo[PostEvent]).toList)
        case _ =>
          deserializationError("Invalid JSON: you may be missing some fields, see documentation")
      }
    }
  }

}

object JsonSupport extends JsonSupport
