package com.flixdb.cdc.scaladsl

import java.io.Closeable

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, Timers}
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.codahale.metrics.SharedMetricRegistries
import com.flixdb.cdc._
import com.flixdb.cdc.scaladsl.PostgreSQLActor._
import javax.sql.DataSource
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

object ChangeDataCapture {
  val metricsRegistry = SharedMetricRegistries.getOrCreate("com.flixdb.cdc")
}

case class ChangeDataCapture(dataSource: DataSource with Closeable)(implicit system: ActorSystem) {

  import ChangeDataCapture._
  import system.dispatcher

  private val logger = LoggerFactory.getLogger(classOf[ChangeDataCapture])

  private val postgreSQL = PostgreSQL(dataSource)

  private lazy val postgreSQLActor = system.actorOf(
    Props(new PostgreSQLActor(postgreSQL))
      .withDispatcher("cdc-blocking-io-dispatcher")
  )

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
    Source
      .unfoldResourceAsync[List[ChangeSet], NotUsed](
        create = () => {
          import settings._
          (postgreSQLActor ? Start(slotName, plugin.name, createSlotOnStart))
            .mapTo[Done]
            .map(_ => NotUsed)
        },
        read = _ => {
          def read() = {
            import settings._
            (postgreSQLActor ? GetChanges(
              slotName,
              mode,
              plugin,
              maxItems,
              columnsToIgnore
            )).mapTo[ChangeSetList].map(_.list).map(Some(_))
          }
          delayed(settings.pollInterval).flatMap(_ => read())
        },
        close = _ => {
          import settings._
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
        (postgreSQLActor ? Flush(settings.slotName, items.last.logSeqNum)).mapTo[Done]
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
        (postgreSQLActor ? Flush(settings.slotName, items.last._2.logSeqNum))
          .mapTo[Done]
          .map(_ => items.toList)
      }
  }.mapConcat(identity)

}

object PostgreSQLActor {

  case class Start(slotName: String, pluginName: String, createSlotIfNotExists: Boolean)

  case class GetChanges(
      slotName: String,
      mode: Mode,
      plugin: Plugin,
      maxItems: Int,
      columnsToIgnore: Map[String, List[String]]
  )

  case class Stop(delay: Option[FiniteDuration], slotName: String, dropSlot: Boolean, closeDataSource: Boolean)

  case class Flush(slotName: String, logSeqNum: String)

  case class ChangeSetList(list: List[ChangeSet])

}

class PostgreSQLActor(postgreSQL: PostgreSQL) extends Actor with ActorLogging with Timers {

  import PostgreSQLActor._

  override def receive: Receive = {
    case Start(slotName, pluginName, createIfNotExists) =>
      if (!postgreSQL.slotExists(slotName, pluginName))
        if (createIfNotExists)
          postgreSQL.createSlot(slotName, pluginName)
      sender() ! Done

    case Flush(slotName, logSeqNum) =>
      postgreSQL.flush(slotName, logSeqNum)
      sender() ! Done

    case GetChanges(slotName, mode, plugin, maxItems, columnsToIgnore) =>
      val result: List[ChangeSet] = {
        val slotChanges: List[PostgreSQL.SlotChange] =
          postgreSQL.pullChanges(mode, slotName, maxItems)
        plugin match {
          case Plugins.TestDecoding =>
            TestDecodingPlugin.transformSlotChanges(slotChanges, columnsToIgnore)
          case Plugins.Wal2Json =>
            Wal2JsonPlugin.transformSlotChanges(slotChanges, columnsToIgnore)
        }
      }
      sender() ! ChangeSetList(result)

    case Stop(None, slotName, dropSlot, closeDataSource) =>
      if (dropSlot)
        postgreSQL.dropSlot(slotName)
      if (closeDataSource)
        postgreSQL.ds.close()

    case s @ Stop(Some(delay), _, _, _) =>
      timers.startSingleTimer("TickKey", s.copy(delay = None), delay)

  }

}
