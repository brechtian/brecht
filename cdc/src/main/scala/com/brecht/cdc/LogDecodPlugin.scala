package com.brecht.cdc

import com.brecht.cdc.PostgreSQL.SlotChange

trait LogDecodPlugin {
  def transformSlotChanges(
      slotChanges: List[SlotChange],
      colsToIgnorePerTable: Map[String, List[String]]
  ): List[ChangeSet]
}
