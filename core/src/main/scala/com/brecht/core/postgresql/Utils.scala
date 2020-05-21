package com.brecht.core.postgresql

import java.sql._

import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

object SQLCompositeException {

  @scala.annotation.tailrec
  def display(exceptions: List[SQLException], acc: List[String], index: Int): String =
    exceptions match {
      case Nil => acc.reverse.mkString("\n")
      case head :: tail =>
        val current =
          s"${index}: SQLState: ${head.getSQLState} ${head.getClass.getName}: ${head.getMessage}"
        display(tail, current +: acc, index + 1)
    }

  def apply(original: SQLException): SQLCompositeException =
    new SQLCompositeException(
      original,
      unRollExceptions(original, original :: Nil)
    )

  @scala.annotation.tailrec
  private def unRollExceptions(sqlException: SQLException, acc: List[SQLException]): List[SQLException] = {
    val ex = sqlException.getNextException
    if (ex != null) {
      unRollExceptions(ex, ex :: acc)
    } else {
      acc
    }
  }

}

case class SQLCompositeException(sqlException: SQLException, unrolled: List[SQLException])
    extends SQLException(
      s"""Multiple exceptions were thrown (${unrolled.size}): ${SQLCompositeException
        .display(unrolled, Nil, 1)}"""
    ) {

  // https://www.postgresql.org/docs/10/errcodes-appendix.html
  def isUniqueConstraintViolation: Boolean =
    sqlException.getSQLState == "23505"

  def isConcurrencyConflict: Boolean = isUniqueConstraintViolation

  def isUndefinedTable: Boolean =
    sqlException.getSQLState == "42P01"

  def isTimeout: Boolean =
    sqlException match {
      case s: SQLTimeoutException => true
      case _                      => false
    }

}

class JdbcUtils {

  import scala.util.chaining._

  private val logger = LoggerFactory.getLogger(classOf[JdbcUtils])

  def convertException(e: Throwable): Throwable =
    e match {
      case sqlEx: SQLException =>
        SQLCompositeException(sqlEx)
      case other => other
    }

  def attemptCloseConnection(conn: Connection): Unit =
    attemptClose(conn, (c: Connection) => c.isClosed)

  def attemptClose[T <: AutoCloseable](resource: T, isClosed: T => Boolean): Unit = {
    Option(resource) match { // null check
      case Some(res) if !isClosed(res) =>
        Try(res.close()).tap {
          case Failure(e) =>
            logger.error(s"Failed to close resource: ${resource.getClass.getName}", e)
          case _ =>
        }
      case _ => ()
    }
  }

  def attemptCloseStatement(st: Statement): Unit =
    attemptClose(st, (r: Statement) => r.isClosed)

  def attemptRollbackConnection(conn: Connection): Unit = {
    import util.chaining._
    Option(conn) match {
      case Some(c) =>
        Try(c.rollback()).tap {
          case Failure(e) =>
            logger.error("Failed to rollback transaction", e)
          case _ =>
        }
      case None => // nothing to rollback
    }
  }

  def attemptCloseResultSet(rs: ResultSet): Unit =
    attemptClose(rs, (s: ResultSet) => s.isClosed)
}
