package com.flixdb.core.postgresql

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import akka.Done
import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.event.LoggingAdapter
import com.flixdb.core.{EventEnvelope, HikariCP}
import com.zaxxer.hikari.HikariDataSource
import org.postgresql.util.PGobject

import scala.concurrent.Future
import scala.util.control.NonFatal

object PostgresSQLDataAccessLayer {

  private val createTableStatement: String => String = (prefix: String) =>
    s"""|CREATE TABLE IF NOT EXISTS ${prefix}_events(
        |  "event_id" char(36) NOT NULL UNIQUE,
        |  "substream_id" varchar(255) NOT NULL,
        |  "event_type" varchar(255) NOT NULL,
        |  "sequence_num" int4 NOT NULL,
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
    s"""DELETE FROM "${prefix}_events" WHERE "substream_id" = ? AND "stream" = ? AND sequence_num < ?"""

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

}

class PostgresSQLDataAccessLayer()(implicit system: ActorSystem) {

  private[postgresql] def poolName = "postgresql-main-pool"

  private val blockingExecContext: MessageDispatcher = system.dispatchers.lookup("blocking-io-dispatcher")

  private[postgresql] val hikariDataSource: HikariDataSource = HikariCP(system).getPool(poolName)

  private val logger: LoggingAdapter = akka.event.Logging(system, classOf[PostgresSQLDataAccessLayer])

  import PostgresSQLDataAccessLayer._

  private val utils = new JdbcUtils(system)
  import utils._

  def getMaximumPoolSize: Int = hikariDataSource.getMaximumPoolSize

  def closePools(): Future[Done] =
    Future {
      logger.info("Shutting down connection pools")
      hikariDataSource.close()
      Done
    }(blockingExecContext)

  def validate(): Future[Done] =
    Future {
      try {
        hikariDataSource.validate()
        Done
      } catch {
        case NonFatal(e) =>
          logger.error(e, "Failed to connect to PostgreSQL instance")
          throw e
      }
    }(blockingExecContext)

  def snapshot(tablePrefix: String, eventEnvelope: EventEnvelope): Future[Done] = {
    Future {
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
        deleteStatement.setString(1, eventEnvelope.subStreamId)
        deleteStatement.setString(2, eventEnvelope.stream)
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
        attemptCloseStatement(deleteStatement)
        attemptCloseConnection(conn)
      }
    }(blockingExecContext)
  }

  def selectEvents(
      tablePrefix: String,
      stream: String,
      subStreamId: String
  ): Future[List[EventEnvelope]] =
    Future {
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
    }(blockingExecContext)

  def appendEvents(tablePrefix: String, eventEnvelopes: List[EventEnvelope]): Future[Done] =
    batchInsertEvents(tablePrefix, eventEnvelopes)

  private[postgresql] def batchInsertEvents(tablePrefix: String, eventEnvelopes: List[EventEnvelope]): Future[Done] =
    eventEnvelopes match {
      case Nil => Future.successful(Done)
      case _ =>
        Future {
          var conn: Connection = null
          var statement: PreparedStatement = null
          try {
            conn = hikariDataSource.getConnection
            conn.setAutoCommit(false)
            statement = prepStatementForBatchInsert(conn, tablePrefix, eventEnvelopes)
            statement.executeBatch()
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
            attemptCloseStatement(statement)
            attemptCloseConnection(conn)
          }
        }(blockingExecContext)
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

  private[postgresql] def prepStatementForBatchInsert(
      conn: Connection,
      tablePrefix: String,
      ees: List[EventEnvelope]
  ): PreparedStatement = {
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

  def createTablesIfNotExists(prefix: String): Future[Done] =
    Future {
      var conn: Connection = null
      var statement: Statement = null
      try {
        conn = hikariDataSource.getConnection
        conn.setAutoCommit(false)
        statement = conn.createStatement()
        statement.addBatch(createTableStatement(prefix))
        statement.addBatch(createIndexStatement(prefix))
        statement.setQueryTimeout(1) // TODO: move to configuration
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
    }(blockingExecContext)

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
