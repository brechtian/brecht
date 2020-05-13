package com.flixdb.core

import akka.actor.typed.{ExtensionId, _}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, get, onSuccess, path, post, _}
import com.flixdb.core.DtoConversions._
import com.flixdb.core.Dtos.{Event, EventList}
import akka.actor.typed.scaladsl.adapter._

object HttpInterface extends ExtensionId[HttpInterface] {
  override def createExtension(system: ActorSystem[_]): HttpInterface = {
    new HttpInterface(system)
  }
}

class HttpInterface(system: ActorSystem[_]) extends Extension {

  import JsonSupport._

  val controller = EventStoreController(system)

  private val route =
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
                controller.publishEvents(namespace, stream, subStreamId, toEventEnvelopes(stream, subStreamId, events))
              ) { _ => complete(StatusCodes.OK) }
            }
          }
        )
      }
    )

  implicit val sys = system.toClassic

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

}
