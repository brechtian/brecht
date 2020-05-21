package com.brecht.core

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, get, onSuccess, path, post, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.brecht.core.DtoConversions._
import com.brecht.core.Dtos.{Event, EventList}
import com.brecht.core.postgresql.SQLCompositeException

class HttpJsonRoutes(controller: EventStoreController) {

  import JsonSupport._

  private def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case sql: SQLCompositeException if sql.isUndefinedTable =>
        complete(HttpResponse(StatusCodes.NotFound))
      case other: Exception =>
        complete(HttpResponse(StatusCodes.InternalServerError))
    }

  private[core] val routes: Route =
    handleExceptions(myExceptionHandler)(
      concat(
        path(Segment) { namespace: String =>
          post {
            onSuccess(controller.createNamespace(namespace)) { _ => complete(StatusCodes.OK) }
          }
        },
        path(Segment / "events" / Segment / Segment) { (namespace: String, stream: String, subStreamId: String) =>
          concat(
            get {
              onSuccess(controller.getEvents(namespace, stream, subStreamId)) { events =>
                complete(StatusCodes.OK -> EventList(events.map(e => e: Event)))
              }
            },
            post {
              entity(as[Dtos.PostEventList]) { events: Dtos.PostEventList =>
                onSuccess(
                  controller
                    .publishEvents(namespace, toEventEnvelopes(stream, subStreamId, events))
                ) { _ => complete(StatusCodes.OK) }
              }
            }
          )
        }
      )
    )

}
