package com.flixdb.core

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{as, complete, entity, get, onSuccess, path, post, _}
import akka.http.scaladsl.server.ExceptionHandler
import com.flixdb.core.protobuf.GetMsgs.{PbEventEnvelope, PbGetEventsResult}
import com.flixdb.core.protobuf.PublishMsgs.PbPublishEventsResult
import spray.json.DeserializationException

import scala.concurrent.Future

class HttpInterface(implicit system: ActorSystem) extends JsonSupport {

  private val entitySharding = EventStoreController(system)

  import system.dispatcher

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case deserializationException: DeserializationException =>
        complete(StatusCodes.BadRequest, Map("error" -> deserializationException.getMessage))
      case other: Throwable =>
        complete(StatusCodes.InternalServerError)
    }

  private def handleError(err: PbGetEventsResult.ErrorReason) = {
    err match {
      case e: PbGetEventsResult.ErrorReason.Unrecognized =>
        complete(StatusCodes.InternalServerError)
      case PbGetEventsResult.ErrorReason.UNKNOWN =>
        complete(StatusCodes.InternalServerError)
      case PbGetEventsResult.ErrorReason.NAMESPACE_NOT_FOUND =>
        complete(StatusCodes.NotFound, Map("error" -> "namespace not found"))
      case PbGetEventsResult.ErrorReason.TOO_MANY_REQUESTS =>
        complete(StatusCodes.TooManyRequests, Map("error" -> "too many requests"))
      case PbGetEventsResult.ErrorReason.TIMEOUT =>
        complete(StatusCodes.RequestTimeout, Map("error" -> "request timeout"))
    }
  }

  private def handleError(err: PbPublishEventsResult.ErrorReason) = {
    err match {
      case e: PbPublishEventsResult.ErrorReason.Unrecognized =>
        complete(StatusCodes.InternalServerError)
      case PbPublishEventsResult.ErrorReason.UNKNOWN =>
        complete(StatusCodes.InternalServerError)
      case PbPublishEventsResult.ErrorReason.NAMESPACE_NOT_FOUND =>
        complete(StatusCodes.NotFound, Map("error" -> "namespace not found"))
      case PbPublishEventsResult.ErrorReason.TOO_MANY_REQUESTS =>
        complete(StatusCodes.TooManyRequests, Map("error" -> "too many requests"))
      case PbPublishEventsResult.ErrorReason.TIMEOUT =>
        complete(StatusCodes.RequestTimeout, Map("error" -> "request timeout"))
      case PbPublishEventsResult.ErrorReason.CONCURRENCY_CONFLICT =>
        complete(StatusCodes.Conflict, Map("error" -> "concurrency conflict"))
    }
  }

  private val route =
    concat(
      path(Segment / "events" / Segment / Segment) { (namespace: String, stream: String, entityId: String) =>
        get {
          onSuccess(entitySharding.getEvents(namespace, stream, entityId)) {
            pbGetEventsResult: PbGetEventsResult =>
              pbGetEventsResult.result match {
                case PbGetEventsResult.Result.SUCCESS =>
                  val r = Dtos.EventList(
                    pbGetEventsResult.eventEnvelopes
                      .map((item: PbEventEnvelope) =>
                        Dtos.Event(
                          eventId = item.eventId,
                          entityId = item.entityId,
                          eventType = item.eventType,
                          sequenceNum = item.sequenceNum,
                          data = item.data,
                          stream = item.stream,
                          tags = item.tags.toList,
                          timestamp = item.timestamp
                        )
                      )
                      .toList
                  )
                  complete(StatusCodes.OK, r)
                case PbGetEventsResult.Result.ERROR =>
                  handleError(pbGetEventsResult.errorReason)
                case msg: PbGetEventsResult.Result.Unrecognized =>
                  complete(StatusCodes.InternalServerError)
              }

          }
        }
      },
      path(Segment / "events" / Segment / Segment) { (namespace: String, stream: String, entityId: String) =>
        {
          post {
            entity(as[Dtos.PostEventList]) {
              envelopesToPublish: Dtos.PostEventList =>
                {
                  val request =
                    protobuf.PublishMsgs.PbPublishEventsRequest.defaultInstance
                      .withNamespace(namespace)
                      .withStream(stream)
                      .withEntityId(entityId)
                      .withEventEnvelopes(
                        envelopesToPublish.events.map(envelope =>
                          protobuf.PublishMsgs.PbEventEnvelope(
                            eventId = envelope.eventId,
                            eventType = envelope.eventType,
                            sequenceNum = envelope.sequenceNum,
                            data = envelope.data,
                            tags = envelope.tags.getOrElse(Nil)
                          )
                        )
                      )
                  onSuccess(entitySharding.publishEvents(request)) { pbPublishEventsResult: PbPublishEventsResult =>
                    pbPublishEventsResult.result match {
                      case PbPublishEventsResult.Result.SUCCESS =>
                        complete(StatusCodes.OK)
                      case PbPublishEventsResult.Result.ERROR =>
                        handleError(pbPublishEventsResult.errorReason)
                      case PbPublishEventsResult.Result.Unrecognized(value) =>
                        complete(StatusCodes.InternalServerError)
                    }
                  }
                }
            }
          }
        }
      }
    )

  def start(): Future[Http.ServerBinding] = {

    Http().bindAndHandle(route, "localhost", 8080)
  }

}
