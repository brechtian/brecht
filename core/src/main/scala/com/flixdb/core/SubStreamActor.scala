package com.flixdb.core

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorLogging, Stash}
import akka.pattern.pipe
import com.flixdb.core.PostgreSQLExtensionImpl.PostgreSQLJournalException.TooManyRequests
import com.flixdb.core.PostgreSQLExtensionImpl.PublishEventsResult
import com.flixdb.core.protobuf.GetMsgs._
import com.flixdb.core.protobuf.PublishMsgs._
import com.flixdb.core.protobuf._
import com.flixdb.core.{PostgreSQLExtensionImpl => Journal}

import scala.concurrent.Future
import scala.concurrent.duration._

class SubStreamActor extends Actor with ActorLogging with Stash {

  import SubStreamActor._
  import context.dispatcher

  context.setReceiveTimeout(120.seconds) // TODO: move to configuration

  val journal: Journal = PostgreSQL(context.system)
  var isRecovered = false
  var eventEnvelopes: List[EventEnvelope] = Nil // the current state of the entity

  def id: String = self.path.name

  def triggerRecovery(namespace: String, stream: String, entityId: String): Unit = {
    log.debug("Recovering")
    val f = journal.getEvents(namespace, stream, entityId)
    pipe(f).to(self, sender())
    context.become(recovering)
  }

  override def receive: Receive = {
    case req: PbGetEventsRequest =>
      triggerRecovery(req.namespace, req.stream, req.subStreamId)
      stash()
    case req: PbPublishEventsRequest =>
      triggerRecovery(req.namespace, req.stream, req.subStreamId)
      stash()
    case _ =>
      log.debug("Received unknown message")
  }

  def ready: Receive = {

    case Stop =>
      log.debug("Stopping due to passivation")
      context.stop(self)

    case req: PbGetEventsRequest =>
      log.debug("Got request to get event log, we can respond immediately")
      sender() ! PbGetEventsResult.defaultInstance
        .withResult(PbGetEventsResult.Result.SUCCESS)
        .withEventEnvelopes(toProtobuf(eventEnvelopes))

    case origReq: PbPublishEventsRequest =>
      val req: PbPublishEventsRequest = deduplicate(this.eventEnvelopes, origReq)
      if (isRequestValid(this.eventEnvelopes, req)) {
        // the request is valid
        val timestamp = System.currentTimeMillis()
        val eventEnvelopesToInsert = fromProtobuf(req.stream, req.subStreamId, timestamp, req.eventEnvelopes)
        val f: Future[PublishEventsResult] = journal
          .publishEvents(req.namespace, eventEnvelopesToInsert)
        pipe(f).to(self, sender)
        context.become(waitingForWriteResult)
      } else {
        val result = PbPublishEventsResult.defaultInstance
          .withResult(PbPublishEventsResult.Result.ERROR)
          .withErrorReason(PbPublishEventsResult.ErrorReason.CONCURRENCY_CONFLICT)
        sender() ! result
      }

    case _ =>
      log.debug("Received unknown message")
  }

  def waitingForWriteResult: Receive = {
    case result: PublishEventsResult =>
      sender() ! PbPublishEventsResult.defaultInstance
        .withResult(PbPublishEventsResult.Result.SUCCESS)
      this.eventEnvelopes = this.eventEnvelopes ++ result.eventEnvelopes // TODO: fix this bc it can be slow
      unstashAll()
      context.become(ready)

    case akka.actor.Status.Failure(f: Throwable) =>
      val errorReason = exceptionToPublishEventsErrorReason(f)
      val result = PbPublishEventsResult.defaultInstance
        .withResult(PbPublishEventsResult.Result.ERROR)
        .withErrorReason(errorReason)
      sender() ! result
      unstashAll()
      context.become(ready)

    case msg: PbGetEventsRequest =>
      stash()

    case msg: PbPublishEventsRequest =>
      stash()

    case _ =>
      log.debug("Received unknown message")

  }

  def recovering: Receive = {

    case result: Journal.GetEventsResult =>
      log.info("recovered with {}", result.eventEnvelopes.size)
      isRecovered = true
      this.eventEnvelopes = result.eventEnvelopes
      unstashAll()
      context.become(ready)

    case akka.actor.Status.Failure(f: Throwable) =>
      val errorReason = exceptionToGetEventsErrorReason(f)
      val result = PbGetEventsResult.defaultInstance
        .withResult(PbGetEventsResult.Result.ERROR)
        .withErrorReason(errorReason)
      sender() ! result
      context.stop(self)

    case msg: PbGetEventsRequest =>
      stash()

    case msg: PbPublishEventsRequest =>
      stash()

    case _ =>
      log.debug("Received unknown message")

  }

}

object SubStreamActor {

  def isRequestValid(
      currentState: List[EventEnvelope],
      request: PbPublishEventsRequest
  ): Boolean = {
    validateSeqNumberRange(currentState, request)
    // other things coming soon
  }

  def validateSeqNumberRange(
      eventEnvelopes: List[EventEnvelope],
      req: PbPublishEventsRequest
  ): Boolean = {
    val firstExpectedSeqNum: Int = eventEnvelopes
      .map(_.sequenceNum)
      .maxOption
      .map(i => i + 1)
      .getOrElse(0)
    val expectedSeqRange = firstExpectedSeqNum until firstExpectedSeqNum + req.eventEnvelopes.size
    val actualSeqRange = req.eventEnvelopes.map(_.sequenceNum).sorted
    expectedSeqRange == actualSeqRange
  }

  def toProtobuf(eventEnvelopes: List[EventEnvelope]): List[GetMsgs.PbEventEnvelope] = {
    eventEnvelopes.map(ee => {
      GetMsgs.PbEventEnvelope.defaultInstance
        .withSubStreamId(ee.subStreamId)
        .withData(ee.data)
        .withEventId(ee.eventId)
        .withEventType(ee.eventType)
        .withSequenceNum(ee.sequenceNum)
        .withStream(ee.stream)
        .withTags(ee.tags)
        .withTimestamp(ee.timestamp)

    })
  }

  def fromProtobuf(
      stream: String,
      entityId: String,
      timestamp: Long,
      eventEnvelopes: Seq[PublishMsgs.PbEventEnvelope]
  ): List[EventEnvelope] = {
    eventEnvelopes.map(ee =>
      EventEnvelope(
        eventId = ee.eventId,
        subStreamId = entityId,
        eventType = ee.eventType,
        sequenceNum = ee.sequenceNum,
        data = ee.data,
        stream = stream,
        tags = ee.tags.toList,
        timestamp = timestamp
      )
    )
  }.toList

  def exceptionToPublishEventsErrorReason(ex: Throwable): PublishMsgs.PbPublishEventsResult.ErrorReason = {
    ex match {
      case f: SQLCompositeException if f.isUndefinedTable =>
        PbPublishEventsResult.ErrorReason.NAMESPACE_NOT_FOUND
      case f: SQLCompositeException if f.isTimeout =>
        PbPublishEventsResult.ErrorReason.TIMEOUT
      case f: SQLCompositeException if f.isConcurrencyConflict =>
        PbPublishEventsResult.ErrorReason.CONCURRENCY_CONFLICT
      case f: TooManyRequests =>
        PbPublishEventsResult.ErrorReason.TOO_MANY_REQUESTS
      case other =>
        PbPublishEventsResult.ErrorReason.UNKNOWN
    }
  }

  def exceptionToGetEventsErrorReason(ex: Throwable): GetMsgs.PbGetEventsResult.ErrorReason = {
    ex match {
      case f: SQLCompositeException if f.isUndefinedTable =>
        PbGetEventsResult.ErrorReason.NAMESPACE_NOT_FOUND
      case f: SQLCompositeException if f.isTimeout =>
        PbGetEventsResult.ErrorReason.TIMEOUT
      case f: TooManyRequests =>
        PbGetEventsResult.ErrorReason.TOO_MANY_REQUESTS
      case other =>
        PbGetEventsResult.ErrorReason.UNKNOWN
    }
  }

  def deduplicate(
      currentState: List[EventEnvelope],
      publishEventsRequest: PbPublishEventsRequest
  ): PbPublishEventsRequest = {
    val currentIds = currentState.map(_.eventId)
    publishEventsRequest.withEventEnvelopes(
      publishEventsRequest.eventEnvelopes.filter(e => !currentIds.contains(e.eventId))
    )
  }

}
