package com.flixdb.cdc

import java.io.Closeable

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import javax.sql.DataSource
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

class ChangeDataCapture

object ChangeDataCapture {

  private val logger = LoggerFactory.getLogger(classOf[ChangeDataCapture])

  private def createSlotIfNotExists(
      pg: PostgreSQL,
      settings: PgCdcSourceSettings
  ): Unit = {
    val slotExists = pg.checkSlotExists(settings.slotName, settings.plugin)
    if (!slotExists && settings.createSlotOnStart)
      pg.createSlot(settings.slotName, settings.plugin)
  }

  private def getAndParseChanges(
      pg: PostgreSQL,
      settings: PgCdcSourceSettings
  ): List[ChangeSet] = {
    val result: List[ChangeSet] = {
      val slotChanges: List[PostgreSQL.SlotChange] = pg.pullChanges(settings.mode, settings.slotName, settings.maxItems)
      settings.plugin match {
        case Plugins.TestDecoding => TestDecodingPlugin.transformSlotChanges(slotChanges, settings.columnsToIgnore)
        // leaving room for other plugin implementations
      }
    }
    result
  }

  private val deduplicate: Flow[ChangeSet, ChangeSet, NotUsed] = Flow[ChangeSet].statefulMapConcat { () =>
    var lastSeenTransactionId = Long.MinValue;
    { element: ChangeSet =>
      if (element.transactionId > lastSeenTransactionId) {
        lastSeenTransactionId = element.transactionId
        element :: Nil
      } else {
        logger.debug("Already seen change set with transaction id {}", element.transactionId)
        Nil
      }
    }
  }

  def delayed(delay: FiniteDuration)(implicit system: ActorSystem): Future[Done] = {
    import system.dispatcher
    akka.pattern.after(delay, using = system.scheduler)(Future.successful(Done))
  }

  def source(dataSource: DataSource with Closeable, settings: PgCdcSourceSettings)(
      implicit system: ActorSystem
  ): Source[ChangeSet, NotUsed] = {

    implicit val ec: MessageDispatcher = system.dispatchers.lookup(id = ActorAttributes.IODispatcher.dispatcher)

    Source
      .unfoldResourceAsync[List[ChangeSet], (PostgreSQL, PgCdcSourceSettings)](
        create = () =>
          Future {
            val pg = new PostgreSQL(dataSource)
            createSlotIfNotExists(pg, settings)
            (pg, settings)
          },
        read = s => {
          val pg = s._1
          val settings = s._2
          def f(): Future[Some[List[ChangeSet]]] = Future {
            Some(getAndParseChanges(pg, settings))
          }
          if (settings.pollInterval.toMillis != 0) {
            delayed(settings.pollInterval).flatMap(_ => f())
          } else {
            f()
          }
        },
        close = s => {
          val pg = s._1
          val settings = s._2
          Future {
            if (settings.dropSlotOnFinish) {
              pg.dropSlot(settings.slotName)
            }
            if (settings.closeDataSourceOnFinish) {
              logger.info("Closing data source")
              dataSource.close()
            }
            Done
          }
        }
      )
      .mapConcat(identity)
      .via(deduplicate)

  }

  def ackSink(dataSource: DataSource with Closeable, settings: PgCdcAckSettings)(
      implicit
      system: ActorSystem
  ): Sink[AckLogSeqNum, NotUsed] = {
    val pg = PostgreSQL(dataSource)
    implicit val ec: MessageDispatcher = system.dispatchers.lookup(id = ActorAttributes.IODispatcher.dispatcher)
    Flow[AckLogSeqNum]
      .groupedWithin(settings.maxItems, settings.maxItemsWait)
      .mapAsyncUnordered(1) { items: Seq[AckLogSeqNum] =>
        Future {
          pg.flush(settings.slotName, items.head.logSeqNum)
        }
      }
      .to(Sink.ignore)
  }

  def ackFlow[T](dataSource: DataSource with Closeable, settings: PgCdcAckSettings)(
      implicit
      system: ActorSystem
  ): Flow[(T, AckLogSeqNum), T, NotUsed] = {
    val pg = PostgreSQL(dataSource)
    implicit val ec: MessageDispatcher = system.dispatchers.lookup(id = ActorAttributes.IODispatcher.dispatcher)
    Flow[(T, AckLogSeqNum)]
      .groupedWithin(settings.maxItems, settings.maxItemsWait)
      .mapAsyncUnordered(1) { items: Seq[(T, AckLogSeqNum)] =>
        Future {
          val headElem = items.head
          val passThrough: T = headElem._1
          val ackRequest = headElem._2
          pg.flush(settings.slotName, ackRequest.logSeqNum)
          passThrough
        }
      }
  }

}
