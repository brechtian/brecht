package com.flixdb.cdc.scaladsl

import java.io.Closeable

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.flixdb.cdc._
import javax.sql.DataSource
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

case class ChangeDataCapture()(implicit system: ActorSystem) {

  private val logger = LoggerFactory.getLogger(classOf[ChangeDataCapture])

  import system.dispatcher

  private object BlockingOps {

    val blockingEc: MessageDispatcher =
      system.dispatchers.lookup(id = "cdc-blocking-io-dispatcher")

    def checkSlotExists(pg: PostgreSQL, settings: PgCdcSourceSettings): Future[Boolean] =
      Future {
        pg.checkSlotExists(settings.slotName, settings.plugin)
      }(blockingEc)

    def createSlot(pg: PostgreSQL, settings: PgCdcSourceSettings): Future[Done] =
      Future {
        pg.createSlot(settings.slotName, settings.plugin)
        Done
      }(blockingEc)

    def dropSlotOnFinish(pg: PostgreSQL, settings: PgCdcSourceSettings): Future[Done] = {
      settings.dropSlotOnFinish match {
        case false =>
          Future.successful(Done)
        case true =>
          Future { pg.dropSlot(settings.slotName); Done }(blockingEc)
      }
    }

    def closeDataSourceOnFinish(
        dataSource: DataSource with Closeable,
        settings: PgCdcSourceSettings
    ): Future[Done] = {
      settings.closeDataSourceOnFinish match {
        case false => Future.successful(Done)
        case true =>
          Future {
            logger.info("Closing data source")
            dataSource.close()
            Done
          }(blockingEc)
      }
    }

    def getChanges(
        pg: PostgreSQL,
        settings: PgCdcSourceSettings
    ): Future[List[ChangeSet]] =
      Future {
        val result: List[ChangeSet] = {
          val slotChanges: List[PostgreSQL.SlotChange] =
            pg.pullChanges(settings.mode, settings.slotName, settings.maxItems)
          settings.plugin match {
            case Plugins.TestDecoding => TestDecodingPlugin.transformSlotChanges(slotChanges, settings.columnsToIgnore)
            case Plugins.Wal2Json     => Wal2JsonPlugin.transformSlotChanges(slotChanges, settings.columnsToIgnore)
          }
        }
        result
      }(blockingEc)

    def flush(pg: PostgreSQL, slotName: String, logSeqNum: String): Future[Unit] = {
      Future {
        pg.flush(slotName, logSeqNum)
      }(blockingEc)
    }

  }

  private val deduplicate: Flow[ChangeSet, ChangeSet, NotUsed] = Flow[ChangeSet].statefulMapConcat { () =>
    var lastSeenTransactionId = Long.MinValue;
    { element: ChangeSet =>
      if (element.transactionId > lastSeenTransactionId) {
        lastSeenTransactionId = element.transactionId
        element :: Nil
      } else {
        logger.debug("Already seen ChangeSet with this transaction id {}", element.transactionId)
        Nil
      }
    }
  }

  import BlockingOps._

  def createSlotIfNotExists(
      pg: PostgreSQL,
      settings: PgCdcSourceSettings
  ): Future[Done] = {
    checkSlotExists(pg, settings)
      .flatMap(exists =>
        if ((!exists) && settings.createSlotOnStart)
          createSlot(pg, settings)
        else Future.successful(Done)
      )
  }

  private def delayed(delay: FiniteDuration): Future[Done] = {
    akka.pattern.after(delay, using = system.scheduler)(Future.successful(Done))
  }

  // TODO: integration with DropWizard metrics
  private val monitoringFlow = Flow[ChangeSet].to(Sink.ignore)

  def source(dataSource: DataSource with Closeable, settings: PgCdcSourceSettings)(
      implicit system: ActorSystem
  ): Source[ChangeSet, NotUsed] = {

    val pg = new PostgreSQL(dataSource)

    Source
      .unfoldResourceAsync[List[ChangeSet], NotUsed](
        create = () => createSlotIfNotExists(pg, settings).map(_ => NotUsed),
        read = _ => {
          def f() = getChanges(pg, settings).map(Some(_))
          if (settings.pollInterval.toMillis != 0) {
            delayed(settings.pollInterval)
              .flatMap(_ => f())
          } else {
            f()
          }
        },
        close = _ => {
          delayed(settings.pollInterval.+(250.milliseconds))
            .flatMap(_ => dropSlotOnFinish(pg, settings))
            .flatMap(_ => closeDataSourceOnFinish(dataSource, settings))
        }
      )
      .mapConcat(identity)
      .via(deduplicate)
      .alsoTo(monitoringFlow)

  }

  def ackSink(dataSource: DataSource with Closeable, settings: PgCdcAckSettings)(
      implicit
      system: ActorSystem
  ): Sink[AckLogSeqNum, NotUsed] = {

    val pg = PostgreSQL(dataSource)

    Flow[AckLogSeqNum]
      .groupedWithin(settings.maxItems, settings.maxItemsWait)
      .wireTap(items => {
        val lst = if (logger.isDebugEnabled()) items.map(_.logSeqNum).mkString(",") else null
        logger.debug("List of unacknowledged lsns {}", lst)
      })
      .mapAsyncUnordered(1) { items: Seq[AckLogSeqNum] => flush(pg, settings.slotName, items.last.logSeqNum) }
      .to(Sink.ignore)
  }

  // TODO: use FlowWithContext ?
  def ackFlow[T](dataSource: DataSource with Closeable, settings: PgCdcAckSettings)(
      implicit
      system: ActorSystem
  ): Flow[(T, AckLogSeqNum), (T, AckLogSeqNum), NotUsed] = {
    val pg = PostgreSQL(dataSource)
    Flow[(T, AckLogSeqNum)]
      .groupedWithin(settings.maxItems, settings.maxItemsWait)
      .wireTap(items => {
        val lst = if (logger.isDebugEnabled()) items.map(_._2.logSeqNum).mkString(",") else null
        logger.debug("List of unacknowledged lsns {}", lst)
      })
      .mapAsyncUnordered(1) { items: Seq[(T, AckLogSeqNum)] =>
        flush(pg, settings.slotName, items.last._2.logSeqNum).map(_ => items.toList)
      }
  }.mapConcat(identity)

}
