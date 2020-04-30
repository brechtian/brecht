package com.flixdb.cdc

import scala.concurrent.duration._

sealed trait Mode

object Modes {

  // at most once delivery
  final case object Get extends Mode

  // at least once delivery
  final case object Peek extends Mode

}

sealed trait Plugin {
  val name: String
}

object Plugins {

  final case object TestDecoding extends Plugin {
    override val name = "test_decoding"
  }

}

/** Settings for the PostgreSQL CDC source
  *
  * @param mode Choose between "at most once delivery" / "at least once"
  * @param slotName Name of the logical decoding slot
  * @param createSlotOnStart Create logical decoding slot when the source starts (if it doesn't already exist...)
  * @param dropSlotOnFinish Drop the logical decoding slot when the source stops
  * @param plugin Plugin to use. Only "test_decoding" supported right now.
  * @param columnsToIgnore Columns to ignore
  * @param maxItems Specifies how many rows are fetched in one batch
  * @param pollInterval Duration between polls
  */
final case class PgCdcSourceSettings(
    mode: Mode = Modes.Get,
    slotName: String,
    createSlotOnStart: Boolean = true,
    dropSlotOnFinish: Boolean = false,
    plugin: Plugin = Plugins.TestDecoding,
    columnsToIgnore: Map[String, List[String]] = Map(),
    maxItems: Int = 128,
    pollInterval: FiniteDuration = 2000.milliseconds
) {

  def withMode(mode: Mode): PgCdcSourceSettings =
    copy(mode = mode)

  def withSlotName(slotName: String): PgCdcSourceSettings =
    copy(slotName = slotName)

  def withCreateSlotOnStart(createSlotOnStart: Boolean): PgCdcSourceSettings =
    copy(createSlotOnStart = createSlotOnStart)

  def withPlugin(plugin: Plugin): PgCdcSourceSettings =
    copy(plugin = plugin)

  def withColumnsToIgnore(columnsToIgnore: Map[String, List[String]]): PgCdcSourceSettings =
    copy(columnsToIgnore = columnsToIgnore)

  def withDropSlotOnFinish(dropSlotOnFinish: Boolean): PgCdcSourceSettings =
    copy(dropSlotOnFinish = dropSlotOnFinish)

}

final case class PgCdcAckSettings(
    slotName: String,
    maxItems: Int = 16,
    maxItemsWait: FiniteDuration = 3000.milliseconds
) {

  def withMaxItemsWait(maxItemsWait: FiniteDuration): PgCdcAckSettings =
    copy(maxItemsWait = maxItemsWait)

  def withMaxItems(maxItems: Int): PgCdcAckSettings = copy(maxItems = maxItems)

}
