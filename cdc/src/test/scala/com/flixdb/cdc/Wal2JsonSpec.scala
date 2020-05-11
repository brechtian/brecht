package com.flixdb.cdc

import com.flixdb.cdc.PostgreSQL.SlotChange
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers

class Wal2JsonSpec extends AnyFunSuite with matchers.should.Matchers {

  test("We can filter out columns in the ignore list") {

    Wal2JsonPlugin.filterOutColumns(
      colsToIgnore = List("credit_card", "salary"),
      data = Map("name" -> "Tom", "credit_card" -> "347054704277234", "salary" -> "200000")
    ) shouldBe Map("name" -> "Tom")

  }

  test("We can parse Wal2Json output") {

    val sampleOutput =
      """
        |{
        | "timestamp": "2020-05-03 19:39:13.616952+00",
        | "change":[
        |   {
        |     "kind": "insert",
        |     "schema": "public",
        |     "table": "users",
        |     "columnnames":
        |       ["id", "name", "is_person", "tags", "other_names"],
        |     "columntypes":
        |       ["integer", "character varying(255)", "boolean", "character varying(255)", "character varying[]"],
        |     "columnvalues": [1, "Seb", true, "null", "{Sebastian}"]
        |   }
        | ]
        |}""".stripMargin

    val result = Wal2JsonPlugin.transformSlotChanges(
      SlotChange(transactionId = 42, location = "0/167BBD0", data = sampleOutput) :: Nil,
      colsToIgnorePerTable = Map("users" -> List("other_names"))
    )

    result should have size 1

    val head = result.head
    head.commitLogSeqNum shouldBe "0/167BBD0"
    head.transactionId shouldBe 42
    head.changes should have size 1

    val headChange = head.changes.head
    headChange shouldBe an[RowInserted]

    val rowInserted = headChange.asInstanceOf[RowInserted]

    rowInserted.transactionId shouldBe 42
    rowInserted.commitLogSeqNum shouldBe "0/167BBD0"
    rowInserted.schemaName shouldBe "public"
    rowInserted.tableName shouldBe "users"
    rowInserted.data shouldBe Map("id" -> "1", "name" -> "Seb", "is_person" -> "true", "tags" -> "null")
    rowInserted.schema should have size 4

  }

}
