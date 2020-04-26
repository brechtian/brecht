package com.flixdb.core

import java.sql._

import akka.Done
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.event.LoggingAdapter
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorAttributes, OverflowStrategy, QueueOfferResult, Supervision}
import org.postgresql.util.PGobject

import scala.concurrent.{Future, Promise}
import scala.util.chaining._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

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

final case class SQLCompositeException private (sqlException: SQLException, unrolled: List[SQLException])
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

object PostgreSQLExtensionImpl {

  private val createTableStatement: String => String = (prefix: String) =>
    s"""|CREATE TABLE IF NOT EXISTS ${prefix}_events(
        |  "event_id" char(36) NOT NULL UNIQUE, 
        |  "entity_id" varchar(255) NOT NULL,
        |  "event_type" varchar(255) NOT NULL,
        |  "sequence_num" int4 NOT NULL,
        |  "tstamp" bigint NOT NULL,
        |  "data" jsonb NOT NULL,
        |  "stream" varchar(255) NOT NULL,
        |  "tags" varchar[],
        |  PRIMARY KEY ("sequence_num", "entity_id", "stream")
        |);
        |""".stripMargin

  private val createIndexStatement: String => String = (prefix: String) =>
    s"""|CREATE INDEX IF NOT EXISTS "entity_id_index" ON "${prefix}_events" USING btree (
        |  "entity_id", "stream" );
        |""".stripMargin

  private def selectEventsForEntityIdStatement(prefix: String): String =
    s"""SELECT * FROM "${prefix}_events" WHERE "entity_id" = ? AND "stream" = ? ORDER BY sequence_num ASC"""

  private def getInsertEventStatement(prefix: String): String =
    s"""
       |INSERT INTO ${prefix}_events(
       | "event_id", 
       | "entity_id",
       | "event_type",
       | "sequence_num",
       | "data",
       | "stream",
       | "tags",
       | "tstamp"
       | ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
       |""".stripMargin

  sealed trait Request

  final case class GetEventsRequest(
      namespace: String,
      stream: String,
      entityId: String,
      resultPromise: Promise[GetEventsResult]
  ) extends Request

  final case class GetEventsResult(eventEnvelopes: List[EventEnvelope])

  final case class PublishEventsRequest(
      namespace: String,
      eventEnvelopes: List[EventEnvelope],
      resultPromise: Promise[akka.Done]
  ) extends Request

  final case class PublishEventsResult(eventEnvelopes: List[EventEnvelope])

  abstract class PostgreSQLJournalException(message: String, cause: Throwable) extends Exception(message, cause)

  object PostgreSQLJournalException {

    final case class TooManyRequests(bufferSize: Int, maxInProgress: Int)
        extends PostgreSQLJournalException(
          message = s"Too many requests: buffer size is ~ ${bufferSize}, max in progress is ~ ${maxInProgress}",
          cause = null
        )
  }

}

class PostgreSQLExtensionImpl(system: ActorSystem) extends Extension {

  import PostgreSQLExtensionImpl._
  import com.zaxxer.hikari.HikariDataSource

  implicit val actorSystem = system

  private val blockingExecContext = system.dispatchers.lookup("blocking-io-dispatcher")

  private val logger: LoggingAdapter = akka.event.Logging(system, classOf[PostgreSQLExtensionImpl])

  private[core] def poolName: String = "postgres"

  private[core] val ds: HikariDataSource = HikariCP(system).getPool(poolName)

  private val requestsBufferSize = 1000 // TODO: move to configuration

  def closePools(): Future[Done] =
    Future {
      logger.info("Shutting down connection pools")
      ds.close()
      Done
    }(blockingExecContext)

  def validate(): Future[Done] =
    Future {
      try {
        ds.validate()
        Done
      } catch {
        case NonFatal(e) =>
          logger.error(e, "Failed to connect to PostgreSQL instance")
          throw e
      }
    }(blockingExecContext)

  private[core] def selectEvents(tablePrefix: String, stream: String, entityId: String): Future[List[EventEnvelope]] =
    Future {
      var conn: Connection = null
      var statement: PreparedStatement = null
      var resultSet: ResultSet = null
      try {
        conn = ds.getConnection
        statement = conn.prepareStatement(selectEventsForEntityIdStatement(tablePrefix))
        statement.setQueryTimeout(1) // TODO: move to configuration
        statement.setString(1, entityId)
        statement.setString(2, stream)
        resultSet = statement.executeQuery()
        val result: List[EventEnvelope] = unrollResultSetIntoEventEnvelopes(resultSet, Nil)
        result
      } catch {
        case NonFatal(e: Throwable) =>
          val ex = convertException(e)
          logger.error(ex, "Failed to get events for entity with id {}", entityId)
          throw ex
      } finally {
        attemptCloseResultSet(resultSet)
        attemptCloseStatement(statement)
        attemptCloseConnection(conn)
      }
    }(blockingExecContext)

  private[core] def appendEvents(tablePrefix: String, eventEnvelopes: List[EventEnvelope]): Future[Done] =
    batchInsertEvents(tablePrefix, eventEnvelopes)

  private def batchInsertEvents(tablePrefix: String, eventEnvelopes: List[EventEnvelope]): Future[Done] =
    eventEnvelopes match {
      case Nil => Future.successful(Done)
      case _ =>
        Future {
          var conn: Connection = null
          var statement: PreparedStatement = null
          try {
            conn = ds.getConnection
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

  private def prepStatementForBatchInsert(
      conn: Connection,
      tablePrefix: String,
      ees: List[EventEnvelope]
  ): PreparedStatement = {
    val statement = conn.prepareStatement(getInsertEventStatement(tablePrefix))
    statement.setQueryTimeout(1) // TODO: move to configuration
    ees.foreach { envelope: EventEnvelope =>
      statement.setString(1, envelope.eventId)
      statement.setString(2, envelope.entityId)
      statement.setString(3, envelope.eventType)
      statement.setInt(4, envelope.sequenceNum)
      statement.setObject(5, createPGJsonObject(envelope.data))
      statement.setString(6, envelope.stream)
      statement.setArray(7, conn.createArrayOf("VARCHAR", envelope.tags.toArray))
      statement.setLong(8, envelope.timestamp)
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
        conn = ds.getConnection
        conn.setAutoCommit(false)
        statement = conn.createStatement()
        import PostgreSQLExtensionImpl._
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

  private def convertException(e: Throwable): Throwable =
    e match {
      case sqlEx: SQLException =>
        SQLCompositeException(sqlEx)
      case other => other
    }

  private def attemptCloseConnection(conn: Connection): Unit =
    attemptClose(conn, (c: Connection) => c.isClosed)

  private def attemptClose[T <: AutoCloseable](resource: T, isClosed: T => Boolean): Unit = {
    Option(resource) match { // null check
      case Some(res) if !isClosed(res) =>
        Try(res.close()).tap {
          case Failure(e) =>
            logger.error(e, "Failed to close resource: {}", resource.getClass.getName)
          case _ =>
        }
      case _ => ()
    }
  }

  private def attemptCloseStatement(st: Statement): Unit =
    attemptClose(st, (r: Statement) => r.isClosed)

  private def attemptRollbackConnection(conn: Connection): Unit = {
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

  private def attemptCloseResultSet(rs: ResultSet): Unit =
    attemptClose(rs, (s: ResultSet) => s.isClosed)

  private def resultSetToEventEnvelope(rs: ResultSet): EventEnvelope =
    // "event_id" char(36) NOT NULL
    // "entity_id" varchar(255) NOT NULL
    // "event_type" varchar(255) NOT NULL
    // "sequence_num" int4 NOT NULL
    // "tstamp" timestamp(6) NOT NULL DEFAULT now()
    // "data" jsonb NOT NULL
    // "stream" varchar(255) NOT NULL
    // "tags" varchar[]
    EventEnvelope(
      eventId = rs.getString("event_id"),
      entityId = rs.getString("entity_id"),
      eventType = rs.getString("event_type"),
      sequenceNum = rs.getInt("sequence_num"),
      timestamp = rs.getLong("tstamp"),
      data = rs.getString("data"),
      stream = rs.getString("stream"),
      tags = rs.getArray("tags").getArray.asInstanceOf[scala.Array[String]].toList
    )

  @scala.annotation.tailrec
  private def unrollResultSetIntoEventEnvelopes(rs: ResultSet, acc: List[EventEnvelope]): List[EventEnvelope] =
    if (!rs.next()) {
      acc.reverse
    } else {
      val ee = resultSetToEventEnvelope(rs)
      unrollResultSetIntoEventEnvelopes(rs, ee :: acc)
    }

  private val alwaysResumeDecider: Supervision.Decider = { t: Throwable =>
    logger.info("Caught error but resuming")
    Supervision.Resume
  }

  private val requestsFlow = {
    import system.dispatcher
    Flow[Request]
      .mapAsyncUnordered(ds.getMaximumPoolSize) { req: Request =>
        req match {
          case request: PublishEventsRequest =>
            val result = appendEvents(request.namespace, request.eventEnvelopes)
              .transformWith {
                case Failure(ex) =>
                  request.resultPromise.complete(Failure(ex))
                  Future.failed(ex)
                case Success(Done) =>
                  request.resultPromise.complete(Success(Done))
                  Future.successful(request.eventEnvelopes)
              }
            result
          case request: GetEventsRequest =>
            selectEvents(request.namespace, request.stream, request.entityId)
              .map(r => GetEventsResult(r))
              .transformWith {
                case r @ Success(_) =>
                  request.resultPromise.complete(r)
                  Future.successful(Done)
                case f @ Failure(exception) =>
                  request.resultPromise.complete(f)
                  Future.failed(exception)
              }
        }
      }
      .withAttributes(ActorAttributes.supervisionStrategy(alwaysResumeDecider))
  }

  private val requestsStream = Source
    .queue[Request](requestsBufferSize, OverflowStrategy.dropNew)
    .via(requestsFlow)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  def publishEvent(namespace: String, eventEnvelope: EventEnvelope): Future[PublishEventsResult] = {
    publishEvents(namespace, eventEnvelope :: Nil)
  }

  def publishEvents(namespace: String, eventEnvelopes: List[EventEnvelope]): Future[PublishEventsResult] = {
    import system.dispatcher
    val p = Promise[Done]
    requestsStream
      .offer(PublishEventsRequest(namespace, eventEnvelopes, p))
      .flatMap {
        case QueueOfferResult.Enqueued =>
          p.future
        case QueueOfferResult.Dropped =>
          val ex = PostgreSQLJournalException.TooManyRequests(requestsBufferSize, ds.getMaximumPoolSize)
          p.complete(Failure(ex)).future
        case QueueOfferResult.Failure(cause) =>
          p.complete(Failure(cause)).future
        case QueueOfferResult.QueueClosed =>
          val ex = new java.lang.Exception("Request queue is closed")
          p.complete(Failure(ex)).future
      }
      .map { _ => PublishEventsResult(eventEnvelopes) }
      .recoverWith {
        case e: Throwable =>
          logger.error(e, "Failed to publish events")
          Future.failed(e)
      }
  }

  def getEvents(namespace: String, stream: String, entityId: String): Future[GetEventsResult] = {
    import system.dispatcher
    val p = Promise[GetEventsResult]
    requestsStream
      .offer(GetEventsRequest(namespace, stream, entityId, p))
      .flatMap {
        case QueueOfferResult.Enqueued =>
          p.future
        case QueueOfferResult.Dropped =>
          val ex = PostgreSQLJournalException.TooManyRequests(requestsBufferSize, ds.getMaximumPoolSize)
          p.complete(Failure(ex)).future
        case QueueOfferResult.Failure(cause) =>
          p.complete(Failure(cause)).future
        case QueueOfferResult.QueueClosed =>
          val ex = new java.lang.Exception("Request queue is closed")
          p.complete(Failure(ex)).future
      }
      .recoverWith {
        case e: Throwable =>
          logger.error(e, "Failed to get events")
          Future.failed(e)
      }
  }

}

object PostgreSQL extends ExtensionId[PostgreSQLExtensionImpl] with ExtensionIdProvider {
  override def lookup = PostgreSQL
  override def createExtension(system: ExtendedActorSystem): PostgreSQLExtensionImpl =
    new PostgreSQLExtensionImpl(system)
}
