package com.flixdb.core.postgresql

import akka.Done
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.event.LoggingAdapter
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorAttributes, OverflowStrategy, QueueOfferResult, Supervision}
import com.flixdb.core.{EventEnvelope, FlixDbConfiguration}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

object PostgreSQL extends ExtensionId[PostgreSQLExtensionImpl] with ExtensionIdProvider {

  override def lookup: PostgreSQL.type = PostgreSQL

  override def createExtension(system: ExtendedActorSystem): PostgreSQLExtensionImpl =
    new PostgreSQLExtensionImpl(system)
}

object PostgreSQLExtensionImpl {

  sealed trait Result

  sealed trait Request

  sealed trait ResultPromise[T <: Result] {
    val resultPromise: Promise[T]
  }

  final case class GetEventsRequest(
      namespace: String,
      stream: String,
      subStreamId: String,
      resultPromise: Promise[GetEventsResult]
  ) extends Request
      with ResultPromise[GetEventsResult]

  final case class GetEventsResult(eventEnvelopes: List[EventEnvelope]) extends Result

  final case class PublishEventsRequest(
      namespace: String,
      eventEnvelopes: List[EventEnvelope],
      resultPromise: Promise[PublishEventsResult]
  ) extends Request
      with ResultPromise[PublishEventsResult]

  final case class SnapshotRequest(
      namespace: String,
      eventEnvelope: EventEnvelope,
      resultPromise: Promise[SnapshotResult]
  ) extends Request
      with ResultPromise[SnapshotResult]

  final case class PublishEventsResult(eventEnvelopes: List[EventEnvelope]) extends Result

  final case class SnapshotResult(snapshot: EventEnvelope) extends Result

  abstract class PostgreSQLJournalException(message: String, cause: Throwable) extends Exception(message, cause)

  object PostgreSQLJournalException {

    final case class TooManyRequests(bufferSize: Int, maxInProgress: Int)
        extends PostgreSQLJournalException(
          message = s"Too many requests: buffer size is ~ ${bufferSize}, max in progress is ~ ${maxInProgress}",
          cause = null
        )
  }

}

class PostgreSQLExtensionImpl(system: ActorSystem) extends Extension {

  import PostgreSQLExtensionImpl._

  implicit val actorSystem = system

  private val logger: LoggingAdapter = akka.event.Logging(system, classOf[PostgreSQLExtensionImpl])

  private val flixDbConfiguration = FlixDbConfiguration(system)

  private val dataAccess = new PostgresSQLDataAccessLayer()

  private val requestsBufferSize = flixDbConfiguration.requestQueueSize

  private val alwaysResumeDecider: Supervision.Decider = { t: Throwable =>
    logger.info("Caught error but resuming")
    Supervision.Resume
  }

  def validate(): Future[Done] = dataAccess.validate()

  def createTablesIfNotExists(namespace: String): Future[Done] = dataAccess.createTablesIfNotExists(namespace)

  private def completeRequest[T <: Result](requestWithResultPromise: ResultPromise[T], f: () => Future[T])(
      implicit ec: ExecutionContext,
      tag: ClassTag[T]
  ): Future[T] = {
    f().transformWith {
      case f @ Failure(throwable: Throwable) =>
        requestWithResultPromise.resultPromise.complete(f)
        Future.failed(throwable)
      case s @ Success(value: T) =>
        requestWithResultPromise.resultPromise.complete(s)
        Future.successful(value)
    }
  }

  private val requestsFlow = {
    import system.dispatcher
    Flow[Request]
      .mapAsyncUnordered(dataAccess.getMaximumPoolSize) { req: Request =>
        req match {
          case request: PublishEventsRequest =>
            def f: Future[PublishEventsResult] =
              dataAccess
                .appendEvents(request.namespace, request.eventEnvelopes)
                .map(_ => PublishEventsResult(request.eventEnvelopes))

            completeRequest(request, () => f)

          case request: GetEventsRequest =>
            def f: Future[GetEventsResult] =
              dataAccess
                .selectEvents(request.namespace, request.stream, request.subStreamId)
                .map(r => GetEventsResult(r))

            completeRequest(request, () => f)

          case request: SnapshotRequest =>
            def f: Future[SnapshotResult] =
              dataAccess
                .snapshot(request.namespace, request.eventEnvelope)
                .map(_ => SnapshotResult(request.eventEnvelope))

            completeRequest(request, () => f)
        }
      }
      .withAttributes(ActorAttributes.supervisionStrategy(alwaysResumeDecider))
  }

  private val requestsStream = Source
    .queue[Request](requestsBufferSize, OverflowStrategy.dropNew)
    .via(requestsFlow)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  def runThrough[T <: Result](q: SourceQueueWithComplete[Request])(req: Request with ResultPromise[T]): Future[T] = {
    import system.dispatcher
    val p = req.resultPromise
    q.offer(req).flatMap {
      case QueueOfferResult.Enqueued =>
        p.future
      case QueueOfferResult.Dropped =>
        val ex = PostgreSQLJournalException.TooManyRequests(requestsBufferSize, dataAccess.getMaximumPoolSize)
        p.complete(Failure(ex)).future
      case QueueOfferResult.Failure(cause: Throwable) =>
        p.complete(Failure(cause)).future
      case QueueOfferResult.QueueClosed =>
        val ex = new java.lang.Exception("Request queue is closed")
        p.complete(Failure(ex)).future
    }
  }

  def getEvents(namespace: String, stream: String, subStreamId: String): Future[GetEventsResult] = {
    import system.dispatcher
    val request = GetEventsRequest(namespace, stream, subStreamId, Promise[GetEventsResult]())
    runThrough(requestsStream)(request)
      .recoverWith {
        case e: Throwable =>
          logger.error(e, "Failed to get events")
          Future.failed(e)
      }
  }

  def publishEvent(namespace: String, eventEnvelope: EventEnvelope): Future[PublishEventsResult] = {
    publishEvents(namespace, eventEnvelope :: Nil)
  }

  def publishEvents(namespace: String, eventEnvelopes: List[EventEnvelope]): Future[PublishEventsResult] = {
    import system.dispatcher
    val request = PublishEventsRequest(namespace, eventEnvelopes, Promise[PublishEventsResult]())
    runThrough(requestsStream)(request)
      .recoverWith {
        case e: Throwable =>
          logger.error(e, "Failed to publish events")
          Future.failed(e)
      }
  }

  def snapshot(namespace: String, eventEnvelope: EventEnvelope): Future[SnapshotResult] = {
    import system.dispatcher
    val request = SnapshotRequest(namespace, eventEnvelope, Promise[SnapshotResult]())
    runThrough(requestsStream)(request)
      .recoverWith {
        case e: Throwable =>
          logger.error(e, "Failed to snapshot")
          Future.failed(e)
      }
  }

}
