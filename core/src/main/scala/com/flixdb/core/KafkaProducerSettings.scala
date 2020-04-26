package com.flixdb.core

import java.util.Properties

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable

object KafkaProducerSettings extends ExtensionId[KafkaProducerSettingsImpl] with ExtensionIdProvider {

  override def lookup = KafkaProducerSettings

  override def createExtension(system: ExtendedActorSystem) =
    new KafkaProducerSettingsImpl(system)

}

class KafkaProducerSettingsImpl(system: ExtendedActorSystem) extends Extension {

  def toMap(config: Config): Map[String, String] = {
    val map = mutable.Map[String, String]()
    config.entrySet.forEach((e) => map.addOne(e.getKey, config.getString(e.getKey)))
    map.toMap
  }

  def getSettings: ProducerSettings[String, String] = {
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProperties(toMap(system.settings.config.getConfig("kafka")))

  }

}
