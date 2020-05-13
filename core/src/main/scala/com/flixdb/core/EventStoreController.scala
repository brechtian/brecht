package com.flixdb.core

import akka.Done
import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import com.flixdb.core.postgresql.PostgreSQL
import com.flixdb.core.postgresql.PostgreSQLActor._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object EventStoreController extends ExtensionId[EventStoreController] {
  override def createExtension(system: ActorSystem[_]): EventStoreController =
    new EventStoreController(system)
}

class EventStoreController(system: ActorSystem[_]) extends Extension {

  private val postgreSQL = PostgreSQL(system)

  implicit val timeout = akka.util.Timeout(3.seconds)

  implicit val sys = system

  implicit val ec = system.executionContext

  def createNamespace(namespace: String): Future[Done] = {
    val askResult = postgreSQL.router.ask[CreateTablesResult](ref => CreateTablesIfNotExists(namespace, ref))
    askResult.map(_.result).flatMap {
      case Success(value: Done)     => Future.successful(value)
      case Failure(exception: Throwable) => Future.failed(exception)
    }
  }

  def getEvents(namespace: String, stream: String, subStreamId: String): Future[List[EventEnvelope]] = {
    val askResult = postgreSQL.router.ask[GetEventsResult](ref => GetEventsRequest(namespace, stream, subStreamId, ref))
    askResult.map(_.eventEnvelopes).flatMap {
      case Success(value)     => Future.successful(value)
      case Failure(exception) => Future.failed(exception)
    }
  }

  def publishEvents(
      namespace: String,
      stream: String,
      subStreamId: String,
      eventEnvelopes: List[EventEnvelope]
  ): Future[Done] = {
    val askResult =
      postgreSQL.router.ask[PublishEventsResult](ref => PublishEventsRequest(namespace, eventEnvelopes, ref))
    askResult.map(_.result).flatMap {
      case Success(value: Done)          => Future.successful(result = value)
      case Failure(exception: Throwable) => Future.failed(exception)
    }
  }

}
