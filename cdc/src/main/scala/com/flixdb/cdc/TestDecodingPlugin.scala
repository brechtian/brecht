package com.flixdb.cdc

import java.time._

import com.flixdb.cdc.PostgreSQL.SlotChange
import fastparse.NoWhitespace._
import fastparse._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

private[cdc] case class TestDecodingPlugin()

private[cdc] object TestDecodingPlugin extends LogDecodPlugin {

  private val log = LoggerFactory.getLogger(classOf[TestDecodingPlugin])

  /*
  What we get from PostgreSQL is something like the following:
  location  | xid |                     data
 -----------+-----+-----------------------------------------------
  0/16E0478 | 689 | BEGIN 689
  0/16E0478 | 689 | table public.data: INSERT: id[integer]:1 data[text]:'1'
  0/16E0580 | 689 | table public.data: INSERT: id[integer]:2 data[text]:'2'
  0/16E0650 | 689 | COMMIT 689
  The grammar below is for parsing what is inside the rows of the data column.
   */

  case class BeginStatement(number: Long)

  case class CommitStatement(number: Long, zonedDateTime: ZonedDateTime)

  case class Field(columnName: String, columnType: String, value: String)

  def singleQuote[_: P]: P[Unit] = P("'")

  def doubleQuote[_: P]: P[Unit] = P("\"")

  def digit[_: P]: P[Unit] = P(CharIn("0-9"))

  def lowerCaseLetter[_: P]: P[Unit] = P(CharIn("a-z"))

  def upperCaseLetter[_: P]: P[Unit] = P(CharIn("A-Z"))

  def letter[_: P]: P[Unit] = P(lowerCaseLetter | upperCaseLetter)

  def numberLong[_: P]: P[Long] = P(digit.rep(1).!.map(_.toLong))

  def numberInt[_: P]: P[Int] = P(digit.rep(1).!.map(_.toInt))

  def space[_: P]: P[Unit] = P(" ")

  def twoDigitInt[_: P]: P[Int] = P(digit.rep(exactly = 2).!.map(_.toInt))

  def fourDigitInt[_: P]: P[Int] = P(digit.rep(exactly = 4).!.map(_.toInt))

  def date[_: P]: P[LocalDate] = P(fourDigitInt ~ "-" ~ twoDigitInt ~ "-" ~ twoDigitInt).map { t =>
    LocalDate.of(t._1, t._2, t._3)
  }

  def time[_: P]: P[(LocalTime, ZoneId)] =
    P(twoDigitInt ~ ":" ~ twoDigitInt ~ ":" ~ twoDigitInt ~ "." ~ numberInt ~ (P("+" | "-") ~ digit.rep(1)).!)
      .map { t => LocalTime.of(t._1, t._2, t._3, t._4) -> ZoneOffset.of(t._5) }

  def timestamp[_: P]: P[ZonedDateTime] = P(date ~ space ~ time).map(s => ZonedDateTime.of(s._1, s._2._1, s._2._2))

  def begin[_: P]: P[BeginStatement] = P("BEGIN" ~ space ~ numberLong).map(BeginStatement)

  // matches a commit message like COMMIT 2380 (at 2018-04-09 17:56:36.730413+00)
  // captures the commit number and the time
  def commit[_: P]: P[CommitStatement] =
    P("COMMIT" ~ space ~ numberLong ~ space ~ "(" ~ "at" ~ space ~ timestamp ~ ")").map(CommitStatement.tupled)

  def twoDoubleQuotes[_: P]: P[Unit] = P(CharIn("""""").rep(2, null, 2))
  def escapeDoubleQuote[_: P] = P("\\\"")
  def escape[_: P]: P[Unit] = P(twoDoubleQuotes | escapeDoubleQuote)

  // matches "Scala", "'Scala'" or "The ""Scala"" language", "The \"Scala\" language" etc
  // captures the full string
  def doubleQuotedString[_: P]: P[String] = {
    P(doubleQuote ~ P((!(escape | doubleQuote) ~ AnyChar) | escape).rep ~ doubleQuote).!
  }

  def twoSingleQuoutes[_: P]: P[Unit] = P("''")
  def escapeSingleQuote[_: P]: P[Unit] = P("\\\'")
  def escape2[_: P]: P[Unit] = P(twoSingleQuoutes | escapeSingleQuote)
  // matches 'Scala', 'The ''Scala'' language' etc.
  // captures what's inside the single quotes (i.e., for 'Scala' it captures Scala)
  def singleQuotedString[_: P]: P[String] = {
    P(singleQuote ~ P((!(escape2 | singleQuote) ~ AnyChar) | escape2).rep.! ~ singleQuote)
  }

  // captures my_column_name or something_else
  def unquotedIdentifier[_: P]: P[String] = P(lowerCaseLetter | digit | "$" | "_").rep(1).!

  // captures my_column_name or "MY_COLUMN_NAME"
  def identifier[_: P]: P[String] = P(unquotedIdentifier | doubleQuotedString)

  def arraySyntax[_: P] = P("[" ~ digit.rep(0) ~ "]")

  // matches [character varying], [integer], [xml], [text[]], [text[][]], [integer[]], [integer[3]], [integer[3][3]] etc.
  // captures what's inside the square brackets
  def typeDeclaration[_: P]: P[String] = {
    "[" ~ P(space | letter | digit | arraySyntax).rep.! ~ "]"
  }

  // matches 42, 3.14, true, false or 'some string value'
  def value[_: P]: P[String] = P(P(!(space | singleQuote) ~ AnyChar).rep(1).! | singleQuotedString)

  def changeType[_: P]: P[String] = P("INSERT" | "UPDATE" | "DELETE").!

  // matches a[integer]:1 b[integer]:1 c[integer]:3
  def data[_: P]: P[List[Field]] =
    P(identifier ~ typeDeclaration ~ ":" ~ value)
      .rep(min = 1, sep = space)
      .map(s => s.map(f => Field(f._1, f._2, f._3)))
      .map(_.toList)

  // when we have both the old version and the new version of the row
  def both[_: P]: P[(List[Field], List[Field])] = P(
    "old-key:" ~ space ~ data ~ space ~ "new-tuple:" ~ space ~ data
  )

  // when we have only the new version of the row
  def latest[_: P]: P[(List[Field], List[Field])] = data.map(v => (List.empty[Field], v))

  // note: we need to wrap the function in an abstract class to get rid of a type erasure problem
  abstract class ChangeBuilder extends (((String, Long)) => Change) // (location: String, transactionId: Long) => Change

  def changeStatement[_: P]: P[ChangeBuilder] = {
    val getData: List[Field] => Map[String, String] = fieldList => fieldList.map(f => f.columnName -> f.value).toMap
    val getSchema: List[Field] => Map[String, String] = fieldList =>
      fieldList.map(f => f.columnName -> f.columnType).toMap
    P(s"table" ~ space ~ identifier ~ "." ~ identifier ~ ":" ~ space ~ changeType ~ ":" ~ space ~ P(latest | both))
      .map { m =>
        {
          val schemaName = m._1
          val tableName = m._2
          val fields = m._4
          new ChangeBuilder {
            override def apply(info: (String, Long)): Change =
              m._3 match {
                case "INSERT" =>
                  RowInserted(
                    schemaName = schemaName,
                    tableName = tableName,
                    commitLogSeqNum = info._1,
                    transactionId = info._2,
                    data = getData(fields._2),
                    schema = getSchema(fields._2)
                  )
                case "DELETE" =>
                  RowDeleted(
                    schemaName = schemaName,
                    tableName = tableName,
                    commitLogSeqNum = info._1,
                    transactionId = info._2,
                    data = getData(fields._2),
                    schema = getSchema(fields._2)
                  )
                case "UPDATE" =>
                  RowUpdated(
                    schemaName = schemaName,
                    tableName = tableName,
                    commitLogSeqNum = info._1,
                    transactionId = info._2,
                    dataNew = getData(fields._2),
                    dataOld = getData(fields._1),
                    schemaNew = getSchema(fields._2),
                    schemaOld = getSchema(fields._1)
                  )
              }
          }
        }
      }
  }

  def statement[_: P] = P(changeStatement | begin | commit)

  def getColsToIgnoreForTable(tableName: String, colsToIgnorePerTable: Map[String, List[String]]): Set[String] = {
    val colsToAlwaysIgnore: Set[String] = colsToIgnorePerTable.filter { case (k, _) => k == "*" }.values.flatten.toSet
    colsToAlwaysIgnore ++ colsToIgnorePerTable.get(tableName).map(_.toSet).getOrElse(Set.empty)
  }

  def getTablesToIgnore(colsToIgnorePerTable: Map[String, List[String]]): Set[String] = {
    colsToIgnorePerTable.filter { case (_, v) => v == "*" :: Nil }.keys.toSet
  }

  def filterKeys[K,V](data: Map[K, V], predicate: K => Boolean): Map[K, V] = data.filter {
    case (key, _) =>
      predicate(key)
  }

  def slotChangesToChangeSet(
      transactionId: Long,
      slotChanges: List[SlotChange], // non-empty
      colsToIgnorePerTable: Map[String, List[String]]
  ): ChangeSet = {

    val ignoreTables: Set[String] = getTablesToIgnore(colsToIgnorePerTable)

    val result = ArrayBuffer[Change]()
    var instant: Instant = null
    var commitLogSeqNum: String = null

    // the last item is the "COMMIT _ (at _)"
    val lastItem = slotChanges.last
    parse(lastItem.data, commit(_)) match {
      case Parsed.Success(CommitStatement(_, t: ZonedDateTime), _) =>
        instant = t.toInstant
        commitLogSeqNum = lastItem.location
      case f: Parsed.Failure =>
        log.error(s"Failure ${f.toString()} when parsing ${lastItem.data}")
    }

    // we drop the first item and the last item since the first one is just the "BEGIN _" and the last one is the "COMMIT _ (at _)"
    slotChanges.drop(1).dropRight(1).map(s => (s, parse(s.data, changeStatement(_)))).foreach {

      case (_, Parsed.Success(changeBuilder: ChangeBuilder, _)) =>
        val change: Change = changeBuilder((commitLogSeqNum, transactionId))
        if (!ignoreTables.contains(change.tableName)) {
          val notHidden: String => Boolean =
            f => !getColsToIgnoreForTable(change.tableName, colsToIgnorePerTable).contains(f)
          result += (change match {
            case insert: RowInserted =>
              insert.copy(data = filterKeys(insert.data, notHidden))
            case delete: RowDeleted =>
              delete.copy(data = filterKeys(delete.data, notHidden))
            case update: RowUpdated =>
              update
                .copy(
                  dataNew = filterKeys(update.dataNew, notHidden),
                  dataOld = filterKeys(update.dataOld, notHidden)
                )
          })
        }

      case (s, f: Parsed.Failure) =>
        log.error(s"Failure ${f.toString()} when parsing ${s.data}")

    }

    ChangeSet(transactionId, slotChanges.last.location, instant, result.toList)
  }

  override def transformSlotChanges(
      slotChanges: List[SlotChange],
      colsToIgnorePerTable: Map[String, List[String]]
  ): List[ChangeSet] =
    slotChanges
      .groupBy(_.transactionId)
      .map {
        case (transactionId: Long, changesByTransactionId: List[SlotChange]) =>
          slotChangesToChangeSet(transactionId, changesByTransactionId, colsToIgnorePerTable)
      }
      .filter(_.changes.nonEmpty)
      .toList
      .sortBy(_.transactionId)

}
