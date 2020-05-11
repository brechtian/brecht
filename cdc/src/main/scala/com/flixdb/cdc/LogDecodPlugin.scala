package com.flixdb.cdc

import com.flixdb.cdc.PostgreSQL.SlotChange

trait LogDecodPlugin {
  def transformSlotChanges(
      slotChanges: List[SlotChange],
      colsToIgnorePerTable: Map[String, List[String]]
  ): List[ChangeSet]
}
