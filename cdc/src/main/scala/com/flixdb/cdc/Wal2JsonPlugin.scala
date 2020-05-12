package com.flixdb.cdc

import java.time.{Instant, ZonedDateTime}

import com.flixdb.cdc.PostgreSQL.SlotChange
import org.slf4j.LoggerFactory

private[cdc] case class Wal2JsonPlugin()

private[cdc] object Wal2Json {

  import spray.json._

  case class OldKeys(keyNames: List[String], keyTypes: List[String], keyValues: List[String])

  implicit object OldKeysJsonReader extends RootJsonReader[OldKeys] {
    override def read(json: JsValue): OldKeys = {
      val requiedFields = List("keynames", "keytypes", "keyvalues")
      json.asJsObject.getFields(requiedFields: _*) match {
        case Seq(
            JsArray(keyNamesJsArray),
            JsArray(keyTypesJsArray),
            JsArray(keyValuesJsArray)
            ) =>
          OldKeys(
            keyNames = keyNamesJsArray.collect { case JsString(s: String) => s }.toList,
            keyTypes = keyTypesJsArray.collect { case JsString(s: String) => s }.toList,
            keyValues = keyValuesJsArray.collect {
              case JsNumber(n)  => n.toString()
              case JsNull       => "null"
              case JsString(s)  => s
              case JsBoolean(b) => b.toString
              case _            => deserializationError("Error when deserializing Wal2Json 'keyvalues'.")
            }.toList
          )
        case _ =>
          deserializationError(
            s"Error when deserializing Wal2Json 'oldkeys'. Required fields: ${requiedFields.mkString(",")}"
          )
      }
    }
  }

  case class Insert(
      schema: String,
      table: String,
      columnNames: List[String],
      columnTypes: List[String],
      columnValues: List[String]
  ) extends Change

  case class Update(
      schema: String,
      table: String,
      columnNames: List[String],
      columnTypes: List[String],
      columnValues: List[String],
      oldKeys: OldKeys
  ) extends Change

  case class Delete(schema: String, table: String, oldKeys: OldKeys) extends Change

  sealed trait Change {
    val schema: String
    val table: String
  }

  implicit object ChangeJsonReader extends RootJsonReader[Change] {

    def read(value: JsValue) = {
      val jsObject = value.asJsObject
      val requiredFields = List("kind", "schema", "table")
      val (kind, schema, table) = jsObject.getFields(requiredFields: _*) match {
        case Seq(
            JsString(kindStr),
            JsString(schemaStr),
            JsString(tableStr)
            ) =>
          (kindStr, schemaStr, tableStr)
        case _ =>
          deserializationError(
            s"Error when deserializing Wal2Json change. Required fields: ${requiredFields.mkString(",")}"
          )
      }

      var columnNames = List.empty[String]
      var columnValues = List.empty[String]
      var columnTypes = List.empty[String]
      var oldKeys = OldKeys(Nil, Nil, Nil)

      jsObject.fields.foreach {
        case ("columnnames", JsArray(columnNamesJsArray)) =>
          columnNames = columnNamesJsArray.collect { case JsString(s: String) => s }.toList
        case ("columntypes", JsArray(columnTypesJsArray)) =>
          columnTypes = columnTypesJsArray.collect { case JsString(s: String) => s }.toList
        case ("columnvalues", JsArray(columnValuesJsArray)) =>
          columnValues = columnValuesJsArray.collect {
            case JsNumber(n)  => n.toString()
            case JsNull       => "null"
            case JsString(s)  => s
            case JsBoolean(b) => b.toString
            case _            => deserializationError("Error when deserializing Wal2Json 'columnvalues'.")
          }.toList
        case ("oldkeys", jsObj: JsObject) =>
          oldKeys = jsObj.convertTo[OldKeys]
        case _ => // ignore
      }

      kind match {
        case "insert" =>
          Insert(schema, table, columnNames, columnTypes, columnValues)
        case "delete" =>
          Delete(schema, table, oldKeys)
        case "update" =>
          Update(schema, table, columnNames, columnTypes, columnValues, oldKeys)
        case _ =>
          // TODO: handle truncate ?
          deserializationError(s"Error when deserializing Wal2Json, kind ${kind} not known")
      }

    }
  }

  case class Wal2JsonR00t(timestamp: Instant, change: List[Change])

  private def parseTime(timestamp: String): ZonedDateTime = {
    import fastparse._
    parse(timestamp, TestDecodingPlugin.timestamp(_)) match {
      case Parsed.Success(value, _) => value
      case _: Parsed.Failure =>
        throw new Exception(s"Failed to parse timestamp ${timestamp}")
    }
  }

  implicit object RootJsonReader extends RootJsonReader[Wal2JsonR00t] {
    override def read(json: JsValue): Wal2JsonR00t = {
      val jsObject = json.asJsObject
      val expectedFields = List("timestamp", "change")
      jsObject.getFields(expectedFields: _*) match {
        case Seq(
            JsString(timestamp),
            JsArray(changesJsArray)
            ) =>
          Wal2JsonR00t(parseTime(timestamp).toInstant, changesJsArray.collect {
            case jsObj: JsObject =>
              jsObj.convertTo[Change]
          }.toList)
        case _ =>
          deserializationError(
            s"Error when deserializing Wal2Json change. Expected fields: ${expectedFields.mkString(",")}"
          )
      }
    }
  }

}

private[cdc] object Wal2JsonPlugin extends LogDecodPlugin {

  private val log = LoggerFactory.getLogger(classOf[Wal2JsonPlugin])

  import Wal2Json._
  import spray.json._

  private[cdc] def filterOutColumns(
      colsToIgnore: List[String],
      data: Map[String, String]
  ): Map[String, String] = {
    data.collect {
      case item @ (key, _) if !colsToIgnore.contains(key) => item
    }
  }

  private[cdc] def buildMap(names: List[String], values: List[String], colsToIgnore: List[String]) = {
    val initial = names.zip(values).toMap
    val result = filterOutColumns(colsToIgnore, initial)
    result
  }

  private[cdc] def wal2JsonR00tToChangeSet(
      txId: Long,
      lsn: String,
      wal2JsonR00t: Wal2JsonR00t,
      colsToIgnorePerTable: Map[String, List[String]]
  ): ChangeSet = {
    ChangeSet(
      transactionId = txId,
      commitLogSeqNum = lsn,
      instant = wal2JsonR00t.timestamp,
      changes = wal2JsonR00t.change.collect {
        case item if !colsToIgnorePerTable.get(item.table).exists(p => p.contains("*")) =>
          val colsToIgnoreForThisTable: List[String] = colsToIgnorePerTable.getOrElse(item.table, Nil)

          item match {
            case Insert(schema, table, columnNames, columnTypes, columnValues) =>
              val newData = buildMap(columnNames, columnValues, colsToIgnoreForThisTable)
              val newSchema = buildMap(columnNames, columnTypes, colsToIgnoreForThisTable)
              RowInserted(schema, table, lsn, txId, data = newData, schema = newSchema)
            case Update(schema, table, columnNames, columnTypes, columnValues, oldKeys) =>
              val newData = buildMap(columnNames, columnValues, colsToIgnoreForThisTable)
              val newSchema = buildMap(columnNames, columnTypes, colsToIgnoreForThisTable)
              val oldData = buildMap(oldKeys.keyNames, oldKeys.keyValues, colsToIgnoreForThisTable)
              val oldSchema = buildMap(oldKeys.keyNames, oldKeys.keyTypes, colsToIgnoreForThisTable)
              RowUpdated(
                schema,
                table,
                lsn,
                txId,
                dataOld = oldData,
                schemaOld = oldSchema,
                dataNew = newData,
                schemaNew = newSchema
              )
            case Delete(schema, table, oldKeys) =>
              val oldData = buildMap(oldKeys.keyNames, oldKeys.keyValues, colsToIgnoreForThisTable)
              val oldSchema = buildMap(oldKeys.keyNames, oldKeys.keyTypes, colsToIgnoreForThisTable)
              RowDeleted(schema, table, lsn, txId, data = oldData, schema = oldSchema)

          }
      }
    )
  }

  override def transformSlotChanges(
      slotChanges: List[SlotChange],
      colsToIgnorePerTable: Map[String, List[String]]
  ): List[ChangeSet] = {

    val result: List[ChangeSet] = slotChanges.map(slotChange => {
      val wal2JsonR00t = slotChange.data.parseJson.convertTo[Wal2JsonR00t]
      wal2JsonR00tToChangeSet(slotChange.transactionId, slotChange.location, wal2JsonR00t, colsToIgnorePerTable)
    })
    result

  }

}
