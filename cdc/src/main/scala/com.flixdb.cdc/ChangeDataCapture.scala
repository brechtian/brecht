package com.flixdb.cdc

import java.io.Closeable

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import javax.sql.DataSource
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class ChangeDataCapture

object ChangeDataCapture {

  private val logger = LoggerFactory.getLogger(classOf[ChangeDataCapture])

  private def checkSlotExists(pg: PostgreSQL, settings: PgCdcSourceSettings)(
      implicit ec: ExecutionContext
  ): Future[Boolean] = Future {
    pg.checkSlotExists(settings.slotName, settings.plugin)
  }

  private def createSlot(pg: PostgreSQL, settings: PgCdcSourceSettings)(
      implicit ec: ExecutionContext
  ): Future[Done] = Future {
    pg.createSlot(settings.slotName, settings.plugin)
    Done
  }

  private def dropSlotOnFinish(pg: PostgreSQL, settings: PgCdcSourceSettings)(
      implicit ec: ExecutionContext
  ): Future[Done] = {
    settings.dropSlotOnFinish match {
      case false => Future.successful(Done)
      case true  => Future { pg.dropSlot(settings.slotName); Done }
    }
  }

  private def closeDataSourceOnFinish(
      dataSource: DataSource with Closeable,
      settings: PgCdcSourceSettings
  )(implicit ec: ExecutionContext): Future[Done] = {
    settings.closeDataSourceOnFinish match {
      case false => Future.successful(Done)
      case true =>
        Future {
          logger.info("Closing data source")
          dataSource.close()
          Done
        }
    }
  }

  private def createSlotIfNotExists(
      pg: PostgreSQL,
      settings: PgCdcSourceSettings
  )(implicit ec: ExecutionContext): Future[Done] =
    checkSlotExists(pg, settings)
      .flatMap(exists =>
        if ((!exists) && settings.createSlotOnStart)
          createSlot(pg, settings)
        else Future.successful(Done)
      )

  private def getChanges(
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
        logger.debug("Already seen ChangeSet with this transaction id {}", element.transactionId)
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

    val pg = new PostgreSQL(dataSource)

    implicit val ec: MessageDispatcher = system.dispatchers.lookup(id = "cdc-blocking-io-dispatcher")

    Source
      .unfoldResourceAsync[List[ChangeSet], NotUsed](
        create = () => createSlotIfNotExists(pg, settings).map(_ => NotUsed),
        read = _ => {
          def f(): Future[Some[List[ChangeSet]]] = Future {
            Some(getChanges(pg, settings))
          }
          if (settings.pollInterval.toMillis != 0) {
            delayed(settings.pollInterval).flatMap(_ => f())
          } else {
            f()
          }
        },
        close = _ => {
          dropSlotOnFinish(pg, settings)
            .flatMap(_ => closeDataSourceOnFinish(dataSource, settings))
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
    implicit val ec: MessageDispatcher = system.dispatchers.lookup(id = "cdc-blocking-io-dispatcher")
    Flow[AckLogSeqNum]
      .groupedWithin(settings.maxItems, settings.maxItemsWait)
      .wireTap(items => {
        val lst = if (logger.isDebugEnabled()) items.map(_.logSeqNum).mkString(",") else null
        logger.debug("List of unacknowledged lsns {}", lst)
      })
      .mapAsyncUnordered(1) { items: Seq[AckLogSeqNum] =>
        Future {
          pg.flush(settings.slotName, items.last.logSeqNum)
        }
      }
      .to(Sink.ignore)
  }

  def ackFlow[T](dataSource: DataSource with Closeable, settings: PgCdcAckSettings)(
      implicit
      system: ActorSystem
  ): Flow[(T, AckLogSeqNum), T, NotUsed] = {
    val pg = PostgreSQL(dataSource)
    implicit val ec: MessageDispatcher = system.dispatchers.lookup(id = "cdc-blocking-io-dispatcher")
    Flow[(T, AckLogSeqNum)]
      .groupedWithin(settings.maxItems, settings.maxItemsWait)
      .wireTap(items => {
        val lst = if (logger.isDebugEnabled()) items.map(_._2.logSeqNum).mkString(",") else null
        logger.debug("List of unacknowledged lsns {}", lst)
      })
      .mapAsyncUnordered(1) { items: Seq[(T, AckLogSeqNum)] =>
        Future {
          val lastElem: (T, AckLogSeqNum) = items.last
          val passThrough: T = lastElem._1
          val ackRequest = lastElem._2
          pg.flush(settings.slotName, ackRequest.logSeqNum)
          passThrough
        }
      }
  }

}
