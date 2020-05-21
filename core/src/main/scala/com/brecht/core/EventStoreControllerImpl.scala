package com.brecht.core

import akka.Done
import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import com.brecht.core.postgresql.PostgreSQLActor
import com.brecht.core.postgresql.PostgreSQLActor._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait EventStoreController {

  def createNamespace(namespace: String): Future[Done]

  def getEvents(namespace: String, stream: String, subStreamId: String): Future[List[EventEnvelope]]

  def publishEvents(
      namespace: String,
      eventEnvelopes: List[EventEnvelope]
  ): Future[Done]

}

class EventStoreControllerImpl(postgreSQL: ActorRef[PostgreSQLActor.Request])(implicit system: ActorSystem[_])
    extends EventStoreController {

  implicit private val timeout = akka.util.Timeout(3.seconds)

  implicit private val ec = system.executionContext

  override def createNamespace(namespace: String): Future[Done] = {
    val askResult = postgreSQL.ask[CreateTablesResult](ref => CreateTablesIfNotExists(namespace, ref))
    askResult.map(_.result).flatMap {
      case Success(value: Done)          => Future.successful(value)
      case Failure(exception: Throwable) => Future.failed(exception)
    }
  }

  override def getEvents(namespace: String, stream: String, subStreamId: String): Future[List[EventEnvelope]] = {
    val askResult = postgreSQL.ask[GetEventsResult](ref => GetEventsRequest(namespace, stream, subStreamId, ref))
    askResult.map(_.eventEnvelopes).flatMap {
      case Success(value)     => Future.successful(value)
      case Failure(exception) => Future.failed(exception)
    }
  }

  override def publishEvents(
      namespace: String,
      eventEnvelopes: List[EventEnvelope]
  ): Future[Done] = {
    val askResult =
      postgreSQL.ask[PublishEventsResult](ref => PublishEventsRequest(namespace, eventEnvelopes, ref))
    askResult.map(_.result).flatMap {
      case Success(value: Done)          => Future.successful(result = value)
      case Failure(exception: Throwable) => Future.failed(exception)
    }
  }

}
