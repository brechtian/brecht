package com.flixdb.core.postgresql

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import akka.Done
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.event.LoggingAdapter
import com.flixdb.core.{EventEnvelope, HikariCP}
import com.zaxxer.hikari.HikariDataSource
import io.prometheus.client.Histogram
import org.postgresql.util.PGobject

import scala.util.Try
import scala.util.control.NonFatal

object PostgresSQLDataAccess {

  private val createTableStatement: String => String = (prefix: String) =>
    s"""|CREATE TABLE IF NOT EXISTS ${prefix}_events(
        |  "event_id" char(36) NOT NULL UNIQUE,
        |  "substream_id" varchar(255) NOT NULL,
        |  "event_type" varchar(255) NOT NULL,
        |  "sequence_num" int GENERATED BY DEFAULT AS IDENTITY NOT NULL,
        |  "tstamp" bigint NOT NULL,
        |  "data" jsonb NOT NULL,
        |  "stream" varchar(255) NOT NULL,
        |  "tags" varchar[],
        |  "snapshot" boolean NOT NULL DEFAULT FALSE,
        |  PRIMARY KEY ("sequence_num", "substream_id", "stream")
        |);
        |""".stripMargin

  private val createIndexStatement: String => String = (prefix: String) =>
    s"""|CREATE INDEX IF NOT EXISTS "substream_id_index" ON "${prefix}_events" USING btree (
        |  "substream_id", "stream" );
        |""".stripMargin

  private def selectEventsStatement(prefix: String): String =
    s"""SELECT * FROM "${prefix}_events" WHERE "substream_id" = ? AND "stream" = ? ORDER BY "sequence_num" ASC"""

  private def getDeleteEventsStatement(prefix: String, stream: String, subStreamId: String, upToSeqNum: Int): String =
    s"""DELETE FROM "${prefix}_events" WHERE "stream" = ? AND "substream_id" = ? AND sequence_num < ?"""

  private def getInsertEventStatement(prefix: String): String =
    s"""
       |INSERT INTO "${prefix}_events"(
       | "event_id",
       | "substream_id",
       | "event_type",
       | "sequence_num",
       | "data",
       | "stream",
       | "tags",
       | "tstamp",
       | "snapshot"
       | ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
       |""".stripMargin

  private def getInsertEventStatementWithDefaultSeqNum(prefix: String): String =
    s"""
       |INSERT INTO "${prefix}_events"(
       | "event_id",
       | "substream_id",
       | "event_type",
       | "sequence_num",
       | "data",
       | "stream",
       | "tags",
       | "tstamp",
       | "snapshot"
       | ) VALUES(?, ?, ?, DEFAULT, ?, ?, ?, ?, ?)
       |""".stripMargin

  val getEventsLatency = Histogram
    .build()
    .help("Request latency in seconds.")
    .name("get_events_latency")
    .labelNames("namespace", "stream", "substream_id")
    .register

  val snapshotLatency = Histogram.build
    .help("Request latency in seconds.")
    .name("snapshot_latency")
    .labelNames("namespace", "event_type", "stream", "substream_id")
    .register()

  val appendLatency = Histogram.build
    .help("Request latency in seconds.")
    .name("append_latency")
    .labelNames("namespace", "event_type")
    .register()

}

class PostgresSQLDataAccess()(implicit system: ActorSystem) {

  private[postgresql] def poolName = "postgresql-main-pool"

  private[postgresql] val hikariDataSource: HikariDataSource = HikariCP(system.toTyped).startHikariDataSource(poolName)

  private val logger: LoggingAdapter = akka.event.Logging(system, classOf[PostgresSQLDataAccess])

  import PostgresSQLDataAccess._

  private val utils = new JdbcUtils(system)
  import utils._

  def getMaximumPoolSize: Int = hikariDataSource.getMaximumPoolSize

  def closePools(): Done = {
    logger.info("Shutting down connection pools")
    hikariDataSource.close()
    Done
  }

  def validate(): Done = {
    try {
      hikariDataSource.validate()
      Done
    } catch {
      case NonFatal(e) =>
        logger.error(e, "Failed to connect to PostgreSQL instance")
        throw e
    }
  }

  def trySnapshot(tablePrefix: String, eventEnvelope: EventEnvelope): Try[Done] = {
    val requestTimer = snapshotLatency
      .labels(tablePrefix, eventEnvelope.eventType, eventEnvelope.stream, eventEnvelope.subStreamId)
      .startTimer
    val result = Try(snapshot(tablePrefix, eventEnvelope))
    requestTimer.observeDuration()
    result
  }

  def snapshot(tablePrefix: String, eventEnvelope: EventEnvelope): Done = {
    var conn: Connection = null
    var deleteStatement: PreparedStatement = null
    var insertStatement: PreparedStatement = null
    try {
      conn = hikariDataSource.getConnection
      conn.setAutoCommit(false)
      insertStatement = prepStatementForSingleInsert(conn, tablePrefix, eventEnvelope)
      deleteStatement = conn.prepareStatement(
        getDeleteEventsStatement(
          tablePrefix,
          stream = eventEnvelope.stream,
          subStreamId = eventEnvelope.subStreamId,
          upToSeqNum = eventEnvelope.sequenceNum
        )
      )
      deleteStatement.setString(1, eventEnvelope.stream)
      deleteStatement.setString(2, eventEnvelope.subStreamId)
      deleteStatement.setInt(3, eventEnvelope.sequenceNum)
      deleteStatement.executeUpdate()
      insertStatement.execute()
      conn.commit()
      Done
    } catch {
      case NonFatal(e) =>
        val ex = convertException(e)
        logger.error(ex, s"Failed to save snapshot into table {}_events", tablePrefix)
        attemptRollbackConnection(conn)
        throw ex
    } finally {
      attemptCloseStatement(insertStatement)
      attemptCloseStatement(deleteStatement)
      attemptCloseConnection(conn)
    }
  }

  def trySelectEvents(tablePrefix: String, stream: String, subStreamId: String) = {
    val requestTimer = getEventsLatency
      .labels(tablePrefix, stream, subStreamId)
      .startTimer
    val result = Try(selectEvents(tablePrefix, stream, subStreamId))
    requestTimer.observeDuration()
    result
  }

  def selectEvents(
      tablePrefix: String,
      stream: String,
      subStreamId: String
  ): List[EventEnvelope] = {
    var conn: Connection = null
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      conn = hikariDataSource.getConnection
      statement = conn.prepareStatement(selectEventsStatement(tablePrefix))
      statement.setQueryTimeout(1)
      statement.setString(1, subStreamId)
      statement.setString(2, stream)
      resultSet = statement.executeQuery()
      val result: List[EventEnvelope] = unrollResultSetIntoEventEnvelopes(resultSet, Nil)
      result
    } catch {
      case NonFatal(e: Throwable) =>
        val ex = convertException(e)
        logger.error(ex, "Failed to get events for sub stream with id {}", subStreamId)
        throw ex
    } finally {
      attemptCloseResultSet(resultSet)
      attemptCloseStatement(statement)
      attemptCloseConnection(conn)
    }
  }

  def tryAppendEvents(tablePrefix: String, eventEnvelopes: List[EventEnvelope]): Try[Done] = {
    val eventType = eventEnvelopes.length match {
      case 0 => "void"
      case 1 => eventEnvelopes.map(_.eventType).head
      case nrEvents if nrEvents <= 7 =>
        val result: String = eventEnvelopes
          .groupBy(_.eventType)
          .map { case (k, v) => s"${k} * ${v.size}" }
          .mkString(", ")
        result
      case nrEvents => s"Batch of ${nrEvents}"
    }
    val requestTimer = appendLatency
      .labels(tablePrefix, eventType)
      .startTimer
    val result = Try(appendEvents(tablePrefix, eventEnvelopes))
    requestTimer.observeDuration()
    result
  }

  def appendEvents(tablePrefix: String, eventEnvelopes: List[EventEnvelope]): Done =
    batchInsertEvents(tablePrefix, eventEnvelopes)

  private[postgresql] def batchInsertEvents(tablePrefix: String, eventEnvelopes: List[EventEnvelope]): Done =
    eventEnvelopes match {
      case Nil => Done
      case _ =>
        var conn: Connection = null
        var statement1: PreparedStatement = null
        var statement2: PreparedStatement = null
        // we split into two parts:
        // 1 - the events that have the expected sequence number set (optimistic concurrency)
        // 2 - the events that don't have the sequence number set (concurrency conflict don't matter)
        val (eventEnvs1, eventEnvs2) = eventEnvelopes.partition(p => p.sequenceNum != -1)
        try {
          conn = hikariDataSource.getConnection
          conn.setAutoCommit(false)
          statement1 = prepStatementForBatchInsert(conn, tablePrefix, eventEnvs1)
          statement2 = prepStatementForBatchInsertWithDefaultSeqNum(conn, tablePrefix, eventEnvs2)
          statement1.executeBatch()
          statement2.executeBatch()
          conn.commit()
          Done
        } catch {
          case NonFatal(e) =>
            val ex = convertException(e)
            val numEvents = eventEnvelopes.size
            logger.error(ex, s"Failed to insert {} events into table {}_events", numEvents, tablePrefix)
            attemptRollbackConnection(conn)
            throw ex
        } finally {
          attemptCloseStatement(statement1)
          attemptCloseStatement(statement2)
          attemptCloseConnection(conn)
        }
    }

  private[postgresql] def prepStatementForSingleInsert(
      conn: Connection,
      tablePrefix: String,
      envelope: EventEnvelope
  ): PreparedStatement = {
    val statement = conn.prepareStatement(getInsertEventStatement(tablePrefix))
    statement.setQueryTimeout(1)
    statement.setString(1, envelope.eventId)
    statement.setString(2, envelope.subStreamId)
    statement.setString(3, envelope.eventType)
    statement.setInt(4, envelope.sequenceNum)
    statement.setObject(5, createPGJsonObject(envelope.data))
    statement.setString(6, envelope.stream)
    statement.setArray(7, conn.createArrayOf("VARCHAR", envelope.tags.toArray))
    statement.setLong(8, envelope.timestamp)
    statement.setBoolean(9, envelope.snapshot)
    statement
  }
  private[postgresql] def prepStatementForBatchInsertWithDefaultSeqNum(
      conn: Connection,
      tablePrefix: String,
      ees: List[EventEnvelope]
  ): PreparedStatement = {
    assert(!ees.exists(p => p.sequenceNum != -1))
    val statement = conn.prepareStatement(getInsertEventStatementWithDefaultSeqNum(tablePrefix))
    statement.setQueryTimeout(1)
    ees.foreach { envelope: EventEnvelope =>
      statement.setString(1, envelope.eventId)
      statement.setString(2, envelope.subStreamId)
      statement.setString(3, envelope.eventType)
      statement.setObject(4, createPGJsonObject(envelope.data))
      statement.setString(5, envelope.stream)
      statement.setArray(6, conn.createArrayOf("VARCHAR", envelope.tags.toArray))
      statement.setLong(7, envelope.timestamp)
      statement.setBoolean(8, envelope.snapshot)
      statement.addBatch()
    }
    statement
  }

  private[postgresql] def prepStatementForBatchInsert(
      conn: Connection,
      tablePrefix: String,
      ees: List[EventEnvelope]
  ): PreparedStatement = {
    assert(!ees.exists(p => p.sequenceNum == -1))
    val statement = conn.prepareStatement(getInsertEventStatement(tablePrefix))
    statement.setQueryTimeout(1)
    ees.foreach { envelope: EventEnvelope =>
      statement.setString(1, envelope.eventId)
      statement.setString(2, envelope.subStreamId)
      statement.setString(3, envelope.eventType)
      statement.setInt(4, envelope.sequenceNum)
      statement.setObject(5, createPGJsonObject(envelope.data))
      statement.setString(6, envelope.stream)
      statement.setArray(7, conn.createArrayOf("VARCHAR", envelope.tags.toArray))
      statement.setLong(8, envelope.timestamp)
      statement.setBoolean(9, envelope.snapshot)
      statement.addBatch()
    }
    statement
  }

  private def createPGJsonObject(value: String): PGobject = {
    import org.postgresql.util.PGobject
    val jsonObject = new PGobject
    jsonObject.setType("jsonb")
    jsonObject.setValue(value)
    jsonObject
  }

  def tryCreateTablesIfNotExixts(prefix: String): Try[Done] = {
    Try(createTablesIfNotExists(prefix))
  }

  def createTablesIfNotExists(prefix: String) = {
    var conn: Connection = null
    var statement: Statement = null
    try {
      conn = hikariDataSource.getConnection
      conn.setAutoCommit(false)
      statement = conn.createStatement()
      statement.addBatch(createTableStatement(prefix))
      statement.addBatch(createIndexStatement(prefix))
      statement.setQueryTimeout(1)
      val result = statement.executeBatch()
      assert(result.length == 2)
      conn.commit()
      logger.info("Successfully created schema")
      Done
    } catch {
      case NonFatal(e) =>
        val ex = convertException(e)
        logger.error(ex, "Failed to create table {}_events", prefix)
        attemptRollbackConnection(conn)
        throw ex
    } finally {
      attemptCloseStatement(statement)
      attemptCloseConnection(conn)
    }
  }

  private def resultSetToEventEnvelope(rs: ResultSet): EventEnvelope =
    // "event_id" char(36) NOT NULL
    // "substream_id" varchar(255) NOT NULL
    // "event_type" varchar(255) NOT NULL
    // "sequence_num" int4 NOT NULL
    // "tstamp" timestamp(6) NOT NULL DEFAULT now()
    // "data" jsonb NOT NULL
    // "stream" varchar(255) NOT NULL
    // "tags" varchar[]
    // "snapshot" boolean NOT NULL DEFAULT FALSE
    EventEnvelope(
      eventId = rs.getString("event_id"),
      subStreamId = rs.getString("substream_id"),
      eventType = rs.getString("event_type"),
      sequenceNum = rs.getInt("sequence_num"),
      timestamp = rs.getLong("tstamp"),
      data = rs.getString("data"),
      stream = rs.getString("stream"),
      tags = rs.getArray("tags").getArray.asInstanceOf[scala.Array[String]].toList,
      snapshot = rs.getBoolean("snapshot")
    )

  @scala.annotation.tailrec
  private def unrollResultSetIntoEventEnvelopes(rs: ResultSet, acc: List[EventEnvelope]): List[EventEnvelope] =
    if (!rs.next()) {
      acc.reverse
    } else {
      val ee = resultSetToEventEnvelope(rs)
      unrollResultSetIntoEventEnvelopes(rs, ee :: acc)
    }

}
