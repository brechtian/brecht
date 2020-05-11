package com.flixdb.cdc

import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}

import fastparse.{Parsed, _}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers

import scala.collection.Map

class TestDecodingParsingSpec extends AnyFunSuite with matchers.should.Matchers {

  import com.flixdb.cdc.TestDecodingPlugin._

  test("parsing double quoted string") {

    val ex1 = "\"Hello\""
    parse(ex1, doubleQuotedString(_)) should matchPattern { case Parsed.Success("\"Hello\"", _) => }

    val ex2 = "\"Hello \\\"world\\\"\""
    parse(ex2, doubleQuotedString(_)) should matchPattern { case Parsed.Success("\"Hello \\\"world\\\"\"", _) => }

    val ex3 = """"users""""
    parse(ex3, doubleQuotedString(_)) should matchPattern { case Parsed.Success("\"users\"", _) => }

  }

  test("parsing single quoted strings") {

    val ex1 = "'Hello world'"

    parse(ex1, singleQuotedString(_)) should matchPattern { case Parsed.Success("Hello world", _) => }

    val ex2 = "'Hello ''world'''"
    parse(ex2, singleQuotedString(_)) should matchPattern {
      case Parsed.Success("Hello ''world''", _) =>
    }

  }

  test("parsing unquoted strings") {

    val ex1 = "some_thing_42"

    parse(ex1, unquotedIdentifier(_)) should matchPattern {
      case Parsed.Success("some_thing_42", _) =>
    }

    val ex2 = "\""

    parse(ex2, unquotedIdentifier(_)) should not matchPattern { case Parsed.Success(_, _) => }

  }

  test("parsing identifiers") {

    val ex1 = "users"
    parse(ex1, identifier(_)) should matchPattern { case Parsed.Success("users", _) => }

    val ex2 = "\"USERS\""
    parse(ex2, identifier(_)) should matchPattern { case Parsed.Success("\"USERS\"", _) => }

    val ex3 = "'users'"
    parse(ex3, identifier(_)) should not matchPattern { case Parsed.Success(_, _) => }

  }

  test("parsing type declarations") {

    val ex1 = "[integer]"
    parse(ex1, typeDeclaration(_)) should matchPattern { case Parsed.Success("integer", _) => }

    val ex2 = "[character varying]"
    parse(ex2, typeDeclaration(_)) should matchPattern { case Parsed.Success("character varying", _) => }

  }

  test("parsing values") {

    val ex1 = "'scala'"
    parse(ex1, TestDecodingPlugin.value(_)) should matchPattern { case Parsed.Success("scala", _) => }

    val ex2 = "true"
    parse(ex2, TestDecodingPlugin.value(_)) should matchPattern { case Parsed.Success("true", _) => }

    val ex3 = "3.14"
    parse(ex3, TestDecodingPlugin.value(_)) should matchPattern { case Parsed.Success("3.14", _) => }

    val ex4 = """'<foo><bar id="42"></bar></foo>'"""
    parse(ex4, TestDecodingPlugin.value(_)) should matchPattern {
      case Parsed.Success("""<foo><bar id="42"></bar></foo>""", _) =>
    }

    val ex5 = "'<foo>\n<bar id=\"42\">\n</bar>\n</foo>'"
    parse(ex5, TestDecodingPlugin.value(_)) should matchPattern {
      case Parsed.Success("<foo>\n<bar id=\"42\">\n</bar>\n</foo>", _) =>
    }

  }

  test("parsing fields") {

    val ex1 = "a[integer]:1"
    parse(ex1, data(_)) should matchPattern {
      case Parsed.Success(List(Field("a", "integer", "1")), _) =>
    }

    val ex2 = "a[integer]:1 b[integer]:2"
    parse(ex2, data(_)) should matchPattern {
      case Parsed.Success(List(Field("a", "integer", "1"), Field("b", "integer", "2")), _) =>
    }

  }

  test("parsing BEGIN and COMMIT statements") {

    parse("BEGIN 2379", begin(_)) should matchPattern { case Parsed.Success(BeginStatement(2379), _) => }

    parse("2018-04-09", date(_)) should matchPattern { case Parsed.Success(localDate: LocalDate, _) => }

    parse("05:52:42.626311+00", time(_)) should matchPattern {
      case Parsed.Success((localTime: LocalTime, zoneId: ZoneId), _) =>
    }

    parse("COMMIT 2213 (at 2018-04-09 05:52:42.626311+00)", commit(_)) should matchPattern {
      case Parsed.Success(CommitStatement(2213, zonedDateTime: ZonedDateTime), _) =>
    }

  }

  test("parsing UPDATE, INSERT, DELETE log statements") {

    import fastparse._

    def changeStatementTest[_: P] = {
      P(changeStatement).map((changeBuilder: ChangeBuilder) => changeBuilder(("unknown", 0)))
    }

    val ex1 = "table public.abc: UPDATE: a[integer]:1 b[integer]:1 c[integer]:3"

    val ex1ExpectedDataNew = Map("a" -> "1", "b" -> "1", "c" -> "3")
    val ex1ExpectedDataOld = Map.empty[String, String]
    val ex1ExpectedSchemaNew = Map("a" -> "integer", "b" -> "integer", "c" -> "integer")
    val ex1ExpectedSchemaOld = Map.empty[String, String]

    parse(ex1, changeStatementTest(_)) should matchPattern {
      case Parsed
            .Success(
            r @ RowUpdated("public", "abc", "unknown", 0, `ex1ExpectedDataNew`, `ex1ExpectedDataOld`, _, _),
            _
            ) if r.schemaOld == ex1ExpectedSchemaOld && r.schemaNew == ex1ExpectedSchemaNew => // success
    }

    val ex2 = "table public.sales: UPDATE: id[integer]:0 info[jsonb]:'{\"name\": \"alpakka\", \"countries\": [\"*\"]}'"

    val ex2ExpectedDataNew = Map("id" -> "0", "info" -> "{\"name\": \"alpakka\", \"countries\": [\"*\"]}")
    val ex2ExpectedDataOld = Map.empty[String, String]
    val ex2ExpectedSchemaNew = Map("id" -> "integer", "info" -> "jsonb")
    val ex2ExpectedSchemaOld = Map.empty[String, String]

    parse(ex2, changeStatementTest(_)) should matchPattern {
      case Parsed.Success(
          r @ RowUpdated("public", "sales", "unknown", 0, `ex2ExpectedDataNew`, `ex2ExpectedDataOld`, _, _),
          _
          ) if r.schemaOld == ex2ExpectedSchemaOld && r.schemaNew == ex2ExpectedSchemaNew => // success
    }

    val ex3 =
      "table public.abc: UPDATE: old-key: a[integer]:3 b[integer]:2 new-tuple: a[integer]:1 b[integer]:2 c[integer]:3"

    val ex3ExpectedDataNew = Map("a" -> "1", "b" -> "2", "c" -> "3")
    val ex3ExpectedDataOld = Map("a" -> "3", "b" -> "2")
    val ex3ExpectedSchemaNew = Map("a" -> "integer", "b" -> "integer", "c" -> "integer")
    val ex3ExpectedSchemaOld = Map("a" -> "integer", "b" -> "integer")

    parse(ex3, changeStatementTest(_)) should matchPattern {
      case Parsed
            .Success(r @ RowUpdated("public", "abc", "unknown", 0, `ex3ExpectedDataNew`, `ex3ExpectedDataOld`, _, _), _)
          if r.schemaOld == ex3ExpectedSchemaOld && r.schemaNew == ex3ExpectedSchemaNew => // success
    }

  }

}
