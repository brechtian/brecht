package com.flixdb.cdc

import java.sql.Connection

import akka.stream.stage._
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}

import scala.collection.mutable

private[cdc] final class PostgreSQLSourceStage(
    instance: PostgreSQLInstance,
    settings: PgCdcSourceSettings
) extends GraphStage[SourceShape[ChangeSet]] {

  private val out: Outlet[ChangeSet] = Outlet[ChangeSet]("postgresqlcdc.out")

  override def initialAttributes: Attributes =
    super.initialAttributes and ActorAttributes.IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PostgreSQLSourceStageLogic(instance, settings, shape)

  override def shape: SourceShape[ChangeSet] = SourceShape(out)

}

private[cdc] final case class PostgreSQLSourceStageLogic(
    instance: PostgreSQLInstance,
    settings: PgCdcSourceSettings,
    shape: SourceShape[ChangeSet]
) extends TimerGraphStageLogic(shape)
    with StageLogging
    with PostgreSQL {

  private val buffer = new mutable.Queue[ChangeSet]()

  lazy val conn: Connection = getConnection(instance.hikariDataSource)

  override def onTimer(timerKey: Any): Unit =
    retrieveChanges()

  private def retrieveChanges(): Unit = {

    val result: List[ChangeSet] = {
      val slotChanges = pullChanges(settings.mode, instance.slotName, settings.maxItems)
      settings.plugin match {
        case Plugins.TestDecoding => TestDecodingPlugin.transformSlotChanges(slotChanges, settings.columnsToIgnore)(log)
        // leaving room for other plugin implementations
      }
    }

    if (result.nonEmpty) {
      buffer ++= result
      push(out, buffer.dequeue())
    } else if (isAvailable(out))
      scheduleOnce("postgresqlcdc-source-timer", settings.pollInterval)

  }

  private def out: Outlet[ChangeSet] = shape.out

  override def preStart(): Unit = {
    val slotExists = checkSlotExists(instance.slotName, settings.plugin)
    if (!slotExists && settings.createSlotOnStart)
      createSlot(instance.slotName, settings.plugin)
  }

  override def postStop(): Unit = {
    if (settings.dropSlotOnFinish) {
      dropSlot(instance.slotName)
    }
    conn.close()
  }

  setHandler(out, new OutHandler {

    override def onPull(): Unit =
      if (buffer.nonEmpty)
        push(out, buffer.dequeue())
      else
        retrieveChanges()
  })

}
