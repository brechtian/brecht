package com.flixdb.cdc

import java.time.Instant

sealed trait Change {

  val schemaName: String
  val tableName: String

  val commitLogSeqNum: String
  val transactionId: Long

}

final case class RowInserted(
    schemaName: String,
    tableName: String,
    commitLogSeqNum: String,
    transactionId: Long,
    data: Map[String, String],
    schema: Map[String, String]
) extends Change

final case class RowUpdated(
    schemaName: String,
    tableName: String,
    commitLogSeqNum: String,
    transactionId: Long,
    dataNew: Map[String, String],
    dataOld: Map[String, String],
    schemaNew: Map[String, String],
    schemaOld: Map[String, String]
) extends Change

final case class RowDeleted(
    schemaName: String,
    tableName: String,
    commitLogSeqNum: String,
    transactionId: Long,
    data: Map[String, String],
    schema: Map[String, String]
) extends Change

final case class ChangeSet(
    transactionId: Long,
    commitLogSeqNum: String,
    instant: Instant,
    changes: List[Change]
)

final case class AckLogSeqNum(logSeqNum: String)
