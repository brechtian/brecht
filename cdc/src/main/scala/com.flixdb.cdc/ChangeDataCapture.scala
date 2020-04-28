package com.flixdb.cdc

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.dispatch.MessageDispatcher
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import javax.sql.DataSource
import scala.concurrent.duration._

import scala.concurrent.{ExecutionContext, Future}

object ChangeDataCapture {

  private case object Tick

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

  private def postgreSQLSource(dataSource: DataSource, settings: PgCdcSourceSettings)(
      implicit ec: ExecutionContext
  ): Source[PostgreSQL, NotUsed] =
    Source.future {
      Future {
        val pg = new PostgreSQL(dataSource)
        createSlotIfNotExists(pg, settings)
        pg
      }
    }

  private def tickOnIntervalSource(
      pg: PostgreSQL,
      settings: PgCdcSourceSettings
  ): Source[(PostgreSQL, ChangeDataCapture.Tick.type), Cancellable] = {
    import scala.concurrent.duration._
    Source.tick(initialDelay = 0.seconds, interval = settings.pollInterval, Tick).map(t => (pg, t))
  }

  private def getAndParseChangesFlow(dataSource: DataSource, settings: PgCdcSourceSettings)(
      implicit ec: ExecutionContext
  ): Flow[(PostgreSQL, ChangeDataCapture.Tick.type), List[ChangeSet], NotUsed] = {
    Flow[(PostgreSQL, Tick.type)].mapAsyncUnordered(parallelism = 1) {
      case (pg: PostgreSQL, Tick) =>
        Future {
          getAndParseChanges(pg, settings)
        }
    }
  }

  def source(dataSource: DataSource, settings: PgCdcSourceSettings)(
      implicit system: ActorSystem
  ): Source[ChangeSet, NotUsed] = {

    implicit val ec: MessageDispatcher = system.dispatchers.lookup(id = ActorAttributes.IODispatcher.dispatcher)
    postgreSQLSource(dataSource, settings)
      .flatMapConcat(pg => tickOnIntervalSource(pg, settings))
      .via(getAndParseChangesFlow(dataSource, settings))
      .mapConcat(identity)
  }

  def restartSource(dataSource: DataSource, settings: PgCdcSourceSettings)(
      implicit system: ActorSystem
  ): Source[ChangeSet, NotUsed] = {

    val result: Source[ChangeSet, NotUsed] = RestartSource.withBackoff(
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2, // adds 20% "noise" to vary the intervals slightly
      maxRestarts = -1 // a negative number will not cap the amount of restarts
    ) { () => source(dataSource, settings) }

    result
  }

  def ackSink(dataSource: DataSource, settings: PgCdcAckSinkSettings): Sink[AckLogSeqNum, NotUsed] =
    Sink.fromGraph(PostgreSQLAckSinkStage(dataSource, settings))

}
