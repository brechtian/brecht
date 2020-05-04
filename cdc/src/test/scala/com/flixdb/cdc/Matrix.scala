package com.flixdb.cdc

import com.flixdb.cdc.scaladsl.Plugins
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.Wait

abstract class PostgreSQLImageName extends PostgreSQLCapturerSpec {

  def imageName: String

  override val container: GenericContainer[_] = {
    val container =
      new GenericContainer(
        imageName
      )
    container.waitingFor(Wait.forLogMessage(".*ready to accept connections.*\\n", 2))
    container.addExposedPort(5432)
    container.start()
    container
  }
}

// user, password and database for the images at:
// flixdb-docker-images.bintray.io
trait SQLContainerCredentials {
  def password = "docker"
  def userName = "docker"
  def database = "docker"
}

trait UsingTestDecodingPlugin {
  def plugin = Plugins.TestDecoding
}

trait UsingWal2JsonPlugin {
  def plugin = Plugins.Wal2Json
}

class OnPostgreSQLVersion122WithTestDecoding
    extends PostgreSQLImageName
    with SQLContainerCredentials
    with UsingTestDecodingPlugin {
  override def imageName = "flixdb-docker-images.bintray.io/flixdb/postgresql:12.2"
}

class OnPostgreSQLVersion117WithTestDecoding
    extends PostgreSQLImageName
    with SQLContainerCredentials
    with UsingTestDecodingPlugin {
  override def imageName = "flixdb-docker-images.bintray.io/flixdb/postgresql:11.7"
}

class OnPostgreSQLVersion1012WithTestDecoding
    extends PostgreSQLImageName
    with SQLContainerCredentials
    with UsingTestDecodingPlugin {
  override def imageName = "flixdb-docker-images.bintray.io/flixdb/postgresql:10.12"
}

class OnPostgreSQLVersion104WithTestDecoding
    extends PostgreSQLImageName
    with SQLContainerCredentials
    with UsingTestDecodingPlugin {
  override def imageName = "flixdb-docker-images.bintray.io/flixdb/postgresql:9.4"
}

class OnPostgreSQLVersion122WithWal2Json
    extends PostgreSQLImageName
    with SQLContainerCredentials
    with UsingWal2JsonPlugin {
  override def imageName = "flixdb-docker-images.bintray.io/flixdb/postgresql:12.2"
}

