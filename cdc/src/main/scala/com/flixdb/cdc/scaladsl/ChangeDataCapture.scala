package com.flixdb.cdc.scaladsl

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.codahale.metrics.SharedMetricRegistries
import com.flixdb.cdc.PostgreSQLActor._
import com.flixdb.cdc._
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

object ChangeDataCapture {
  val metricsRegistry = SharedMetricRegistries.getOrCreate("com.flixdb.cdc")
}

case class ChangeDataCapture(postgreSQLInstance: PostgreSQLInstance)(implicit system: ActorSystem) {

  import ChangeDataCapture._
  import postgreSQLInstance._

  implicit val actorSystem = system.toTyped
  implicit val ec = actorSystem.executionContext

  private val logger = LoggerFactory.getLogger(classOf[ChangeDataCapture])

  private val deduplicate: Flow[ChangeSet, ChangeSet, NotUsed] = Flow[ChangeSet].statefulMapConcat { () =>
    var lastSeenTransactionId = Long.MinValue;
    { element: ChangeSet =>
      if (element.transactionId <= lastSeenTransactionId) {
        logger.debug("Already seen ChangeSet with this transaction id {}", element.transactionId)
        Nil
      } else {
        lastSeenTransactionId = element.transactionId
        element :: Nil
      }
    }
  }

  private val monitoringFlow = {
    val inserts = metricsRegistry.meter("inserts")
    val deletes = metricsRegistry.meter("deletes")
    val updates = metricsRegistry.meter("updates")
    Flow[ChangeSet]
      .mapConcat(_.changes)
      .wireTap { change =>
        {
          change match {
            case _: RowInserted => inserts.mark()
            case _: RowUpdated  => updates.mark()
            case _: RowDeleted  => deletes.mark()
          }
        }
      }
      .to(Sink.ignore)
  }

  private def delayed(delay: FiniteDuration): Future[Done] = {
    akka.pattern.after(delay, using = system.scheduler)(Future.successful(Done))
  }

  def source(settings: PgCdcSourceSettings): Source[ChangeSet, NotUsed] = {
    implicit val timeout = akka.util.Timeout(3.seconds)
    import settings._
    Source
      .unfoldResourceAsync[List[ChangeSet], NotUsed](
        create = () => {
          (postgreSQLActor.ask[Done](ref => Start(slotName, plugin.name, createSlotOnStart, ref)))
            .map(_ => NotUsed)
        },
        read = _ => {
          def read() = {
            (postgreSQLActor.ask[ChangeSetList](ref => GetChanges(
              slotName,
              mode,
              plugin,
              maxItems,
              columnsToIgnore,
              ref
            ))).map(_.list).map(Some(_))
          }
          delayed(settings.pollInterval).flatMap(_ => read())
        },
        close = _ => {
          postgreSQLActor ! Stop(
            Some(pollInterval.+(250.milliseconds)),
            slotName,
            dropSlotOnFinish,
            closeDataSourceOnFinish
          )
          Future.successful(Done)
        }
      )
      .mapConcat(identity)
      .via(deduplicate)
      .alsoTo(monitoringFlow)

  }

  def ackSink(settings: PgCdcAckSettings)(
      implicit
      system: ActorSystem
  ): Sink[AckLogSeqNum, NotUsed] = {
    implicit val timeout = akka.util.Timeout(3.seconds)
    Flow[AckLogSeqNum]
      .groupedWithin(settings.maxItems, settings.maxItemsWait)
      .wireTap(items => {
        val lst = if (logger.isDebugEnabled()) items.map(_.logSeqNum).mkString(",") else null
        logger.debug("List of unacknowledged lsns {}", lst)
      })
      .mapAsyncUnordered(1) { items: Seq[AckLogSeqNum] =>
        postgreSQLActor.ask[Done](ref =>
          Flush(settings.slotName, items.last.logSeqNum, ref))
      }
      .to(Sink.ignore)
  }

  // TODO: get rid of this and learn how to use FlowWithContext ?
  def ackFlow[T](settings: PgCdcAckSettings)(
      implicit
      system: ActorSystem
  ): Flow[(T, AckLogSeqNum), (T, AckLogSeqNum), NotUsed] = {
    implicit val timeout = akka.util.Timeout(3.seconds)
    Flow[(T, AckLogSeqNum)]
      .groupedWithin(settings.maxItems, settings.maxItemsWait)
      .wireTap(items => {
        val lst = if (logger.isDebugEnabled()) items.map(_._2.logSeqNum).mkString(",") else null
        logger.debug("List of unacknowledged lsns {}", lst)
      })
      .mapAsyncUnordered(1) { items: Seq[(T, AckLogSeqNum)] =>
        postgreSQLActor.ask[Done](ref => Flush(settings.slotName, items.last._2.logSeqNum, ref))
          .map(_ => items.toList)
      }
  }.mapConcat(identity)

}
