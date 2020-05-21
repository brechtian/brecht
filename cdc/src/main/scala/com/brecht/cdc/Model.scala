package com.brecht.cdc

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
) extends Change {

  override def toString: String =
    s"RowInserted(schemaName = $schemaName, tableName = $tableName, commitLogSeqNum = $commitLogSeqNum," +
      s" transactionId = $transactionId, data = $data, schema = $schema)"

}

final case class RowUpdated(
    schemaName: String,
    tableName: String,
    commitLogSeqNum: String,
    transactionId: Long,
    dataNew: Map[String, String],
    dataOld: Map[String, String],
    schemaNew: Map[String, String],
    schemaOld: Map[String, String]
) extends Change {

  override def toString: String =
    s"RowUpdated(schemaName = $schemaName, tableName = $tableName, commitLogSeqNum = $commitLogSeqNum," +
      s" transactionId = $transactionId, dataNew = $dataNew, dataOld = $dataOld, schemaNew = $schemaNew," +
      s" schemaOld = $schemaOld)"
}

final case class RowDeleted(
    schemaName: String,
    tableName: String,
    commitLogSeqNum: String,
    transactionId: Long,
    data: Map[String, String],
    schema: Map[String, String]
) extends Change {

  override def toString: String =
    s"RowDeleted(schemaName = $schemaName, tableName = $tableName, commitLogSeqNum = $commitLogSeqNum," +
      s" transactionId = $transactionId, data = $data, schema = $schema)"

}

final case class ChangeSet(
    transactionId: Long,
    commitLogSeqNum: String,
    instant: Instant,
    changes: List[Change]
) {

  override def toString: String = {
    s"ChangeSet(transactionId = $transactionId, commitLogSeqNum = $commitLogSeqNum, instant = $instant, " +
      s"changes = ${changes})"
  }

}

final case class AckLogSeqNum(logSeqNum: String)
