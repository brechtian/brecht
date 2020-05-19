package com.flixdb.cdc

import java.sql.Connection

import org.postgresql.util.PGobject

/* for testing */
trait FakeDb {

  def truncateTable(conn: Connection, tableName: String): Unit = {
    val st = conn.prepareStatement(s"TRUNCATE $tableName")
    st.execute()
    st.close()
  }

  def createCustomersTable(conn: Connection): Unit = {
    val createStatement =
      conn.prepareStatement("""
                              |CREATE TABLE customers (
                              |  id SERIAL,
                              |  first_name VARCHAR(255) NOT NULL,
                              |  last_name VARCHAR(255) NOT NULL,
                              |  email VARCHAR(255) NOT NULL,
                              |  tags TEXT[] NOT NULL,
                              |  PRIMARY KEY(id)
                              |);""".stripMargin)

    createStatement.execute()
    createStatement.close()
  }

  def truncateCustomers(conn: Connection): Unit = {
    truncateTable(conn, "customers")
  }

  def createSalesTable(conn: Connection): Unit = {
    val createStatement =
      conn.prepareStatement("""
                              |CREATE TABLE sales (
                              | id SERIAL NOT NULL PRIMARY KEY,
                              | info JSONB NOT NULL
                              |);""".stripMargin)
    createStatement.execute()
    createStatement.close()
  }

  def truncateSales(conn: Connection): Unit = {
    truncateTable(conn, "sales")
  }

  def createPurchaseOrdersTable(conn: Connection): Unit = {
    val st = conn.prepareStatement("""
                                     |CREATE TABLE purchase_orders (
                                     | id SERIAL NOT NULL PRIMARY KEY,
                                     | info XML NOT NULL
                                     | );""".stripMargin)
    st.execute()
    st.close()
  }

  def truncatePurchaseOrders(conn: Connection): Unit = {
    truncateTable(conn, "purchase_orders")
  }

  def createEmployeesTable(conn: Connection): Unit = {
    val st = conn.prepareStatement("""
                                     |CREATE TABLE employees (
                                     | id serial NOT NULL PRIMARY KEY,
                                     | name VARCHAR(255) NOT NULL,
                                     | position VARCHAR(255) DEFAULT NULL
                                     |);
                                     |""".stripMargin)
    st.execute()
    st.close()
  }

  def truncateEmployees(conn: Connection): Unit = {
    truncateTable(conn, "employees")
  }

  def createImagesTable(conn: Connection): Unit = {
    val st = conn.prepareStatement("""
                                     |CREATE TABLE images(
                                     | id serial NOT NULL PRIMARY KEY,
                                     | image BYTEA NOT NULL
                                     |);""".stripMargin)
    st.execute()
    st.close()
  }

  def truncateImages(conn: Connection): Unit = {
    truncateTable(conn, "images")
  }

  def createWeatherTable(conn: Connection): Unit = {
    val createTableSt = conn.prepareStatement("""
                                                |CREATE TABLE "WEATHER"(
                                                | id serial NOT NULL PRIMARY KEY,
                                                | city VARCHAR(255) NOT NULL,
                                                | weather VARCHAR(255) NOT NULL
                                                |);""".stripMargin)
    createTableSt.execute()
    createTableSt.close()

    val alterTableSt = conn.prepareStatement("ALTER TABLE \"WEATHER\" REPLICA IDENTITY FULL")
    alterTableSt.execute()
    alterTableSt.close()
  }

  def truncateWeather(conn: Connection): Unit = {
    truncateTable(conn, "\"WEATHER\"")
  }

  def createCountriesTable(conn: Connection): Unit = {
    val createTableSt = conn.prepareStatement(
      """CREATE TABLE countries(name VARCHAR(255) NOT NULL, continent VARCHAR(255) NOT NULL);"""
    )
    createTableSt.execute()
    createTableSt.close()
  }

  def truncateCountries(conn: Connection): Unit = {
    truncateTable(conn, "countries")
  }

  def dropTable(name: String, conn: Connection): Unit = {
    val st = conn.prepareCall(s"DROP TABLE $name")
    st.execute()
    st.close()
  }

  def dropTableCustomers(conn: Connection): Unit = dropTable("customers", conn)

  def dropTableSales(conn: Connection): Unit = dropTable("sales", conn)

  def dropTablePurchaseOrders(conn: Connection): Unit = dropTable("purchase_orders", conn)

  def dropTableEmployees(conn: Connection): Unit = dropTable("employees", conn)

  def dropTableImages(conn: Connection): Unit = dropTable("images", conn)

  def dropTableWeather(conn: Connection): Unit = dropTable(""""WEATHER"""", conn)

  def dropTableCountries(conn: Connection): Unit = dropTable("countries", conn)

  def insertCustomer(
      conn: Connection,
      id: Int,
      fName: String,
      lName: String,
      email: String,
      tags: List[String]
  ): Unit = {
    val insertStatement =
      conn.prepareStatement(
        "INSERT INTO customers(id, first_name, last_name, email, tags) VALUES(?, ?, ?, ?, ?)"
      )
    insertStatement.setInt(1, id)
    insertStatement.setString(2, fName)
    insertStatement.setString(3, lName)
    insertStatement.setString(4, email)
    insertStatement.setArray(5, conn.createArrayOf("text", tags.toArray))
    insertStatement.execute()
    insertStatement.close()
  }

  def updateCustomerEmail(conn: Connection, id: Int, newEmail: String): Unit = {
    val updateStatement =
      conn.prepareStatement("UPDATE customers SET email = ? WHERE Id = ?")
    updateStatement.setString(1, newEmail)
    updateStatement.setInt(2, id)
    updateStatement.execute()
    updateStatement.close()
  }

  def deleteCustomers(conn: Connection): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM customers")
    deleteStatement.execute()
    deleteStatement.close()
  }

  def insertSale(conn: Connection, id: Int, info: String): Unit = {
    val pgObject = new PGobject
    pgObject.setType("jsonb")
    pgObject.setValue(info)
    val insertStatement =
      conn.prepareStatement("INSERT INTO sales(id, info) VALUES (?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setObject(2, pgObject)
    insertStatement.execute()
    insertStatement.close()
  }

  def updateSale(conn: Connection, id: Int, newInfo: String): Unit = {
    val pgObject = new PGobject
    pgObject.setType("jsonb")
    pgObject.setValue(newInfo)
    val updateStatement =
      conn.prepareStatement("UPDATE sales SET info = ? WHERE id = ?;")
    updateStatement.setObject(1, pgObject)
    updateStatement.setInt(2, id)
    updateStatement.execute()
    updateStatement.close()
  }

  def deleteSale(conn: Connection, id: Int): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM sales WHERE id = ?;")
    deleteStatement.setInt(1, id)
    deleteStatement.execute()
    deleteStatement.close()
  }

  def insertPurchaseOrder(conn: Connection, id: Int, info: String): Unit = {
    val pGobject = new PGobject
    pGobject.setType("XML")
    pGobject.setValue(info)
    val insertStatement = conn.prepareStatement("INSERT INTO purchase_orders(id, info) VALUES (?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setObject(2, pGobject)
    insertStatement.execute()
    insertStatement.close()
  }

  def deletePurchaseOrder(conn: Connection, id: Int): Unit = {
    val deleteStatement =
      conn.prepareStatement("DELETE FROM purchase_orders WHERE id = ?;")
    deleteStatement.setInt(1, id)
    deleteStatement.execute()
    deleteStatement.close()
  }

  def insertEmployee(conn: Connection, id: Int, name: String, position: String): Unit = {
    val insertStatement =
      conn.prepareStatement("INSERT INTO employees(id, name, position) VALUES(?, ?, ?);")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, name)
    insertStatement.setString(3, position)
    insertStatement.execute()
    insertStatement.close()
  }

  def updateEmployee(conn: Connection, id: Int, newPosition: String): Unit = {
    val updateStatement =
      conn.prepareStatement("UPDATE employees SET position = ? WHERE id = ?;")
    updateStatement.setString(1, newPosition)
    updateStatement.setInt(2, id)
    updateStatement.execute()
    updateStatement.close()
  }

  def deleteEmployees(conn: Connection): Unit = {
    val st = conn.prepareStatement("DELETE FROM employees;")
    st.execute()
    st.close()
  }

  def insertImage(conn: Connection, id: Int, imageName: String): Unit = {
    val fis = this.getClass.getResourceAsStream(imageName)
    assert(fis != null)
    val insertStatement =
      conn.prepareStatement("INSERT INTO images(id, image) VALUES(?, ?)")
    insertStatement.setInt(1, id)
    insertStatement.setBinaryStream(2, fis)
    insertStatement.execute()
    insertStatement.close()
    fis.close()
  }

  def deleteImages(conn: Connection): Unit = {
    val deleteSt = conn.prepareStatement("DELETE FROM images;")
    deleteSt.execute()
    deleteSt.close()
  }

  def insertWeather(conn: Connection, id: Int, city: String, weather: String): Unit = {
    val insertStatement = conn.prepareStatement("INSERT INTO \"WEATHER\"(id, city, weather) VALUES(?, ?, ?)")
    insertStatement.setInt(1, id)
    insertStatement.setString(2, city)
    insertStatement.setString(3, weather)
    insertStatement.execute()
    insertStatement.close()
  }

  def updateWeather(conn: Connection, id: Int, newWeather: String): Unit = {
    val updateStatement = conn.prepareStatement("UPDATE \"WEATHER\" SET weather = ? WHERE id = ?")
    updateStatement.setString(1, newWeather)
    updateStatement.setInt(2, id)
    updateStatement.execute()
    updateStatement.close()
  }

  def deleteWeathers(conn: Connection): Unit = {
    val deleteSt = conn.prepareStatement("DELETE FROM \"WEATHER\";")
    deleteSt.execute()
    deleteSt.close()
  }

  def insertCountry(conn: Connection, name: String, continent: String): Unit = {
    val insertStatement = conn.prepareStatement("INSERT INTO countries(name, continent) VALUES(?,?)")
    insertStatement.setString(1, name)
    insertStatement.setString(2, continent)
    insertStatement.execute()
    insertStatement.close()
  }

  def updateCountry(conn: Connection, name: String, continent: String): Unit = {
    val updateStatement = conn.prepareStatement("UPDATE countries SET continent = ? WHERE name = ?")
    updateStatement.setString(1, continent)
    updateStatement.setString(2, name)
    updateStatement.execute()
    updateStatement.close()
  }

  def deleteCountry(conn: Connection, name: String): Unit = {
    val deleteSt = conn.prepareStatement("DELETE FROM countries WHERE name = ?")
    deleteSt.setString(1, name)
    deleteSt.execute()
    deleteSt.close()
  }

  def setUpLogicalDecodingSlot(conn: Connection, slotName: String, pluginName: String): Unit = {
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_create_logical_replication_slot('$slotName','${pluginName}')")
    stmt.execute()
    stmt.close()
  }

  def dropLogicalDecodingSlot(conn: Connection, slotName: String): Unit = {
    val stmt = conn.prepareStatement(s"SELECT * FROM pg_drop_replication_slot('$slotName')")
    stmt.execute()
    stmt.close()
  }

  def setTimeZoneUtc(conn: Connection): Unit = {
    val stmt = conn.prepareStatement("SET TIME ZONE 'UTC';")
    stmt.execute()
    stmt.close()
  }

}
