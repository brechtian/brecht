package com.flixdb.cdc

import akka.stream._
import akka.stream.stage._
import javax.sql.DataSource

private[cdc] final case class PostgreSQLAckSinkStage(
    dataSource: DataSource,
    settings: PgCdcAckSinkSettings
) extends GraphStage[SinkShape[AckLogSeqNum]] {

  override def initialAttributes: Attributes = super.initialAttributes and ActorAttributes.IODispatcher

  private val in: Inlet[AckLogSeqNum] = Inlet[AckLogSeqNum]("postgresqlcdc.in")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    PostgreSQLSinkStageLogic(dataSource, settings, shape)

  override def shape: SinkShape[AckLogSeqNum] = SinkShape(in)
}

private[cdc] final case class PostgreSQLSinkStageLogic(
    dataSource: DataSource,
    settings: PgCdcAckSinkSettings,
    shape: SinkShape[AckLogSeqNum]
) extends TimerGraphStageLogic(shape)
    with StageLogging {

  private var items: List[String] = List.empty[String] // LSNs of un-acked items (cannot grow > settings.maxItems)
  // note that these have to be received in order (i.e. can't use mapAsyncUnordered before this)

  private val pg = PostgreSQL(dataSource)

  private def in: Inlet[AckLogSeqNum] = shape.in

  override def onTimer(timerKey: Any): Unit = {
    log.debug("Timer")
    acknowledgeItems()
    scheduleOnce("postgresqlcdc-ack-sink-timer", settings.maxItemsWait)
  }

  private def acknowledgeItems(): Unit =
    items.headOption match {
      case Some(v) =>
        pg.flush(settings.slotName, v)
        items = Nil
      case None =>
        log.debug("No items to acknowledge consumption of")
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
    log.info("Stopped")

}
