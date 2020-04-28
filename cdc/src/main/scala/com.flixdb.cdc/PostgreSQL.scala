package com.flixdb.cdc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import javax.sql.DataSource
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

private[cdc] object PostgreSQL {

  /**
    * Represents a row in the table we get from PostgreSQL when we query
    * SELECT * FROM pg_logical_slot_get_changes(..)
    */
  case class SlotChange(transactionId: Long, location: String, data: String)

}

private[cdc] case class PostgreSQL(ds: DataSource) {

  import PostgreSQL._

  private val log = LoggerFactory.getLogger(classOf[PostgreSQL])

  def getConnection: Connection = {
    ds.getConnection()
  }

  /** Checks that the slot exists */
  def checkSlotExists(slotName: String, plugin: Plugin): Boolean = {

    var conn: Connection = null
    var getReplicationSlots: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = getConnection
      getReplicationSlots = conn.prepareStatement("SELECT * FROM pg_replication_slots WHERE slot_name = ?")
      getReplicationSlots.setString(1, slotName)
      rs = getReplicationSlots.executeQuery()

      if (!rs.next()) {
        log.info("logical replication slot with name {} does not exist", slotName)
        false
      } else {
        val database = rs.getString("database")
        val foundPlugin = rs.getString("plugin")
        foundPlugin match {
          case plugin.name =>
            log.info(
              "found logical replication slot with name {} for database {} using {} plugin",
              slotName,
              database,
              plugin.name
            )
          case _ =>
            log.warn("improper plugin configuration for slot with name {}", slotName)
        }
        true
      }
    } catch {
      case NonFatal(e) =>
        log.error("Failed to check if replication slot exists", e)
        throw e
    } finally {
      attemptCloseStatement(getReplicationSlots)
      attemptCloseResultSet(rs)
      attemptCloseConnection(conn)
    }

  }

  def createSlot(slotName: String, plugin: Plugin): Unit = {
    var conn: Connection = null
    var stmt: PreparedStatement = null

    try {
      conn = getConnection
      log.info("setting up logical replication slot {}", slotName)
      stmt = conn.prepareStatement(s"SELECT * FROM pg_create_logical_replication_slot(?, ?)")
      stmt.setString(1, slotName)
      stmt.setString(2, plugin.name)
      stmt.execute()
    } catch {
      case NonFatal(e) =>
        log.error("Failed to create slot", e)
        throw e
    } finally {
      attemptCloseStatement(stmt)
      attemptCloseConnection(conn)
    }
  }

  def dropSlot(slotName: String): Unit = {
    var conn: Connection = null
    var stmt: PreparedStatement = null

    try {
      conn = getConnection
      log.info("dropping logical replication slot {}", slotName)
      stmt = conn.prepareStatement(s"SELECT * FROM pg_drop_logical_replication_slot(?)")
      stmt.setString(1, slotName)
      stmt.execute()
    } catch {
      case NonFatal(e) =>
        log.error("Failed to drop slot", e)
        throw e
    } finally {
      attemptCloseStatement(stmt)
      attemptCloseConnection(conn)
    }
  }

  private def buildGetSlotChangesStatement(conn: Connection, slotName: String, maxItems: Int): PreparedStatement = {
    val statement: PreparedStatement =
      conn.prepareStatement("SELECT * FROM pg_logical_slot_get_changes(?, NULL, ?, 'include-timestamp', 'on')")
    statement.setString(1, slotName)
    statement.setInt(2, maxItems)
    statement
  }

  private def buildPeekSlotChangesStatement(conn: Connection, slotName: String, maxItems: Int): PreparedStatement = {
    val statement: PreparedStatement =
      conn.prepareStatement("SELECT * FROM pg_logical_slot_peek_changes(?, NULL, ?, 'include-timestamp', 'on')")
    statement.setString(1, slotName)
    statement.setInt(2, maxItems)
    statement
  }

  def flush(slotName: String, upToLogSeqNum: String): Unit = {

    var conn: Connection = null
    var statement: PreparedStatement = null

    try {
      conn = getConnection
      statement = conn.prepareStatement("SELECT 1 FROM pg_logical_slot_get_changes(?,?, NULL)")
      statement.setString(1, slotName)
      statement.setString(2, upToLogSeqNum)
      statement.execute()
    } catch {
      case NonFatal(e) =>
        log.error("Failed to flush", e)
    } finally {
      attemptCloseStatement(statement)
      attemptCloseConnection(conn)
    }
  }

  def pullChanges(mode: Mode, slotName: String, maxItems: Int): List[SlotChange] = {

    var conn: Connection = null
    var pullChangesStatement: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = getConnection
      pullChangesStatement = mode match {
        case Modes.Get  => buildGetSlotChangesStatement(conn, slotName, maxItems)
        case Modes.Peek => buildPeekSlotChangesStatement(conn, slotName, maxItems)
      }
      rs = pullChangesStatement.executeQuery()
      val result = ArrayBuffer[SlotChange]()
      while (rs.next()) {
        val data = rs.getString("data")
        val transactionId = rs.getLong("xid")
        val location = Try(rs.getString("location"))
          .getOrElse(rs.getString("lsn")) // in older versions of PG the column is called "lsn" not "location"
        result += SlotChange(transactionId, location, data)
      }
      pullChangesStatement.close()
      log.debug("Captured {} changes", result.size.toString)
      result.toList
    } catch {
      case NonFatal(e) =>
        log.error("Failed to pull changes", e)
        throw e
    } finally {
      attemptCloseResultSet(rs)
      attemptCloseStatement(pullChangesStatement)
      attemptCloseConnection(conn)
    }
  }

  private def attemptCloseResultSet(rs: ResultSet): Unit =
    attemptClose(rs, (s: ResultSet) => s.isClosed)

  private def attemptCloseStatement(st: Statement): Unit =
    attemptClose(st, (r: Statement) => r.isClosed)

  private def attemptCloseConnection(conn: Connection): Unit =
    attemptClose(conn, (c: Connection) => c.isClosed)

  private def attemptClose[T <: AutoCloseable](resource: T, isClosed: T => Boolean): Unit = {
    Option(resource) match { // null check
      case Some(res) if !isClosed(res) =>
        Try(res.close()) match {
          case Failure(_) =>
            log.error("Failed to close resource: {}", resource.getClass.getName)
          case _ =>
        }
      case _ => ()
    }
  }

}
