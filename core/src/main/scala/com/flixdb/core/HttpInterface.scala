package com.flixdb.core

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{as, complete, entity, get, onSuccess, path, post, _}
import akka.http.scaladsl.server.{ExceptionHandler, StandardRoute}
import com.flixdb.core.protobuf.read.PbGetEventsResult
import com.flixdb.core.protobuf.write.PbPublishEventsResult
import spray.json.DeserializationException
import protobuf._

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

  private def handleError(err: read.ErrorReason): StandardRoute = {
    err match {
      case e: read.ErrorReason.Unrecognized =>
        complete(StatusCodes.InternalServerError)
      case read.ErrorReason.UNKNOWN =>
        complete(StatusCodes.InternalServerError)
      case read.ErrorReason.NAMESPACE_NOT_FOUND =>
        complete(StatusCodes.NotFound, Map("error" -> "namespace not found"))
      case read.ErrorReason.TOO_MANY_REQUESTS =>
        complete(StatusCodes.TooManyRequests, Map("error" -> "too many requests"))
      case read.ErrorReason.TIMEOUT =>
        complete(StatusCodes.RequestTimeout, Map("error" -> "request timeout"))
    }
  }

  private def handleError(err: write.ErrorReason): StandardRoute = {
    err match {
      case e: write.ErrorReason.Unrecognized =>
        complete(StatusCodes.InternalServerError)
      case write.ErrorReason.UNKNOWN =>
        complete(StatusCodes.InternalServerError)
      case write.ErrorReason.NAMESPACE_NOT_FOUND =>
        complete(StatusCodes.NotFound, Map("error" -> "namespace not found"))
      case write.ErrorReason.TOO_MANY_REQUESTS =>
        complete(StatusCodes.TooManyRequests, Map("error" -> "too many requests"))
      case write.ErrorReason.TIMEOUT =>
        complete(StatusCodes.RequestTimeout, Map("error" -> "request timeout"))
      case write.ErrorReason.CONCURRENCY_CONFLICT =>
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
                case read.Result.SUCCESS =>
                  val r = Dtos.EventList(
                    pbGetEventsResult.eventEnvelopes
                      .map((item: read.PbEventEnvelope) =>
                        Dtos.Event(
                          eventId = item.eventId,
                          subStreamId = item.subStreamId,
                          eventType = item.eventType,
                          sequenceNum = item.sequenceNum,
                          data = item.data,
                          stream = item.stream,
                          tags = item.tags.toList,
                          timestamp = item.timestamp,
                          snapshot = false
                        )
                      )
                      .toList
                  )
                  complete(StatusCodes.OK, r)
                case read.Result.ERROR =>
                  handleError(pbGetEventsResult.errorReason)
                case msg: read.Result.Unrecognized =>
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
                    write.PbPublishEventsRequest.defaultInstance
                      .withNamespace(namespace)
                      .withStream(stream)
                      .withSubStreamId(entityId)
                      .withEventEnvelopes(
                        envelopesToPublish.events.map(envelope =>
                          write.PbEventEnvelope(
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
                      case write.Result.SUCCESS =>
                        complete(StatusCodes.OK)
                      case write.Result.ERROR =>
                        handleError(pbPublishEventsResult.errorReason)
                      case write.Result.Unrecognized(value) =>
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
