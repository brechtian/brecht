package com.flixdb.cdc

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.dispatch.MessageDispatcher
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.{ExecutionContext, Future}

object ChangeDataCapture {

  private case object Tick

  private def createSlotIfNotExists(
      pg: PostgreSQL,
      instance: PostgreSQLInstance,
      settings: PgCdcSourceSettings
  ): Unit = {
    val slotExists = pg.checkSlotExists(instance.slotName, settings.plugin)
    if (!slotExists && settings.createSlotOnStart)
      pg.createSlot(instance.slotName, settings.plugin)
  }

  private def getAndParseChanges(
      pg: PostgreSQL,
      instance: PostgreSQLInstance,
      settings: PgCdcSourceSettings
  ): List[ChangeSet] = {
    val result: List[ChangeSet] = {
      val slotChanges: List[PostgreSQL.SlotChange] = pg.pullChanges(settings.mode, instance.slotName, settings.maxItems)
      settings.plugin match {
        case Plugins.TestDecoding => TestDecodingPlugin.transformSlotChanges(slotChanges, settings.columnsToIgnore)
        // leaving room for other plugin implementations
      }
    }
    result
  }

  private def postgreSQLSource(instance: PostgreSQLInstance, settings: PgCdcSourceSettings)(implicit ec: ExecutionContext):Source[PostgreSQL, NotUsed] =
    Source.future {
      Future {
        val pg = new PostgreSQL(instance.hikariDataSource)
        createSlotIfNotExists(pg, instance, settings)
        pg
      }
    }

  private def tickOnIntervalSource(pg: PostgreSQL, settings: PgCdcSourceSettings): Source[(PostgreSQL, ChangeDataCapture.Tick.type), Cancellable] = {
    import scala.concurrent.duration._
    Source.tick(initialDelay = 0.seconds, interval = settings.pollInterval, Tick).map(t => (pg, t))
  }

  private def getAndParseChangesFlow(instance: PostgreSQLInstance, settings: PgCdcSourceSettings)(implicit ec: ExecutionContext): Flow[(PostgreSQL, ChangeDataCapture.Tick.type), List[ChangeSet], NotUsed] = {
    Flow[(PostgreSQL, Tick.type)].mapAsyncUnordered(parallelism = 1) {
      case (pg: PostgreSQL, Tick) =>
        Future {
          getAndParseChanges(pg, instance, settings)
        }
    }
  }

  def source(instance: PostgreSQLInstance, settings: PgCdcSourceSettings)(
      implicit system: ActorSystem
  ): Source[ChangeSet, NotUsed] = {

    implicit val ec: MessageDispatcher = system.dispatchers.lookup(id = ActorAttributes.IODispatcher.dispatcher)
    postgreSQLSource(instance, settings)
      .flatMapConcat(pg => tickOnIntervalSource(pg, settings))
      .via(getAndParseChangesFlow(instance, settings))
      .mapConcat(identity)
  }

  def ackSink(instance: PostgreSQLInstance, settings: PgCdcAckSinkSettings): Sink[AckLogSeqNum, NotUsed] =
    Sink.fromGraph(new PostgreSQLAckSinkStage(instance, settings))

}
