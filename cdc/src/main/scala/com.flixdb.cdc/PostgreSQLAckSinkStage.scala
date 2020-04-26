package com.flixdb.cdc

import java.sql.Connection

import akka.stream._
import akka.stream.stage._

import scala.util.control.NonFatal

private[cdc] final class PostgreSQLAckSinkStage(
    instance: PostgreSQLInstance,
    settings: PgCdcAckSinkSettings
) extends GraphStage[SinkShape[AckLogSeqNum]] {

  override def initialAttributes: Attributes = super.initialAttributes and ActorAttributes.IODispatcher

  private val in: Inlet[AckLogSeqNum] = Inlet[AckLogSeqNum]("postgresqlcdc.in")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PostgreSQLSinkStageLogic(instance, settings, shape)

  override def shape: SinkShape[AckLogSeqNum] = SinkShape(in)
}

private[cdc] final class PostgreSQLSinkStageLogic(
    val instance: PostgreSQLInstance,
    val settings: PgCdcAckSinkSettings,
    val shape: SinkShape[AckLogSeqNum]
) extends TimerGraphStageLogic(shape)
    with StageLogging
    with PostgreSQL {

  private var items: List[String] = List.empty[String] // LSNs of un-acked items (cannot grow > settings.maxItems)
  // note that these have to be received in order (i.e. can't use mapAsyncUnordered before this)

  val conn: Connection = getConnection(instance.hikariDataSource)

  private def in: Inlet[AckLogSeqNum] = shape.in

  override def onTimer(timerKey: Any): Unit = {
    acknowledgeItems()
    scheduleOnce("postgresqlcdc-ack-sink-timer", settings.maxItemsWait)
  }

  private def acknowledgeItems(): Unit =
    items.headOption match {
      case Some(v) =>
        flush(instance.slotName, v)
        items = Nil
      case None =>
        log.debug("no items to acknowledge consumption of")
    }

  override def preStart(): Unit =
    pull(shape.in)

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        val e: AckLogSeqNum = grab(in)
        items = e.logSeqNum :: items
        if (items.size == settings.maxItems)
          acknowledgeItems()
        pull(in)
      }
    }
  )

  override def postStop(): Unit =
    try {
      conn.close()
      log.debug("closed connection")
    } catch {
      case NonFatal(e) =>
        log.error("failed to close connection", e)
    }

}
