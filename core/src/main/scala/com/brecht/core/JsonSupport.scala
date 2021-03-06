package com.brecht.core

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.brecht.core.Dtos._
import spray.json.{DefaultJsonProtocol, JsArray, JsNumber, JsObject, JsString, _}

import scala.language.implicitConversions

object Dtos {

  final case class Event(
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

  final case class PostEvent(
      eventId: String,
      eventType: String,
      sequenceNum: Option[Int],
      data: String,
      tags: Option[List[String]]
  )

  final case class Snapshot(
      eventId: String,
      eventType: String,
      sequenceNum: Int,
      data: String
  )

  final case class EventList(events: List[Event])

  final case class PostEventList(events: List[PostEvent])

}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit object EventJsonWriter extends RootJsonWriter[Event] {
    override def write(ee: Event): JsValue =
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
  }

  implicit object EventListJsonWriter extends RootJsonWriter[EventList] {
    override def write(obj: EventList): JsValue =
      JsArray(obj.events.map(_.toJson).toVector)
  }

  implicit object PostEventJsonReader extends RootJsonReader[PostEvent] {
    override def read(value: JsValue): PostEvent = {
      val jsObject = value.asJsObject
      jsObject.getFields("eventId", "eventType", "data") match {
        case Seq(
            JsString(eventUniqueId),
            JsString(eventType),
            data: JsValue
            ) =>
          // optional fields
          val maybeTags = jsObject.fields.get("tags") match {
            case Some(JsArray(items)) => Some(items.collect { case JsString(v) => v }.toList)
            case None                 => None
            case _                    => deserializationError("Invalid JSON")
          }

          val maybeSequenceNum: Option[Int] = jsObject.fields.get("sequenceNum") match {
            case Some(JsNumber(seqNum)) => Some(seqNum.toIntExact)
            case None                   => None
            case _                      => deserializationError("Invalid JSON")
          }

          PostEvent(eventUniqueId, eventType, maybeSequenceNum, data.compactPrint, maybeTags)

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

  implicit object SnapshotJsonReader extends RootJsonReader[Snapshot] {
    override def read(value: JsValue): Snapshot = {
      val jsObject = value.asJsObject
      jsObject.getFields("eventId", "eventType", "data", "sequenceNum") match {
        case Seq(
            JsString(eventUniqueId),
            JsString(eventType),
            data: JsValue,
            JsNumber(sequenceNum)
            ) =>
          Snapshot(eventUniqueId, eventType, sequenceNum.toIntExact, data.compactPrint)

        case _ =>
          deserializationError("Invalid JSON: you may be missing some fields, see documentation")
      }
    }

  }

}

object DtoConversions {

  implicit def eventEnvelopeToEventDto(eventEnvelope: EventEnvelope): Event = {
    Event(
      eventId = eventEnvelope.eventId,
      subStreamId = eventEnvelope.subStreamId,
      eventType = eventEnvelope.eventType,
      sequenceNum = eventEnvelope.sequenceNum,
      data = eventEnvelope.data,
      stream = eventEnvelope.stream,
      tags = eventEnvelope.tags,
      timestamp = eventEnvelope.timestamp,
      snapshot = eventEnvelope.snapshot
    )
  }

  def toEventEnvelopes(stream: String, subStreamId: String, postEventList: PostEventList) = {
    postEventList.events.map(p =>
      EventEnvelope(
        eventId = p.eventId,
        subStreamId = subStreamId,
        eventType = p.eventType,
        sequenceNum = p.sequenceNum.getOrElse(-1),
        data = p.data,
        stream = stream,
        tags = p.tags.getOrElse(Nil),
        timestamp = 0,
        snapshot = false
      )
    )
  }

  def snapshotDtoToEventEnvelope(stream: String, subStreamId: String, snapshot: Snapshot) = {
    EventEnvelope(
      eventId = snapshot.eventId,
      subStreamId = subStreamId,
      eventType = snapshot.eventType,
      sequenceNum = snapshot.sequenceNum,
      data = snapshot.data,
      stream = stream,
      tags = Nil,
      timestamp = 0,
      snapshot = true
    )
  }
}

object JsonSupport extends JsonSupport
