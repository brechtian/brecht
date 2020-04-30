package com.flixdb.core

import java.util.Properties

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.mutable

object KafkaSettings extends ExtensionId[KafkaSettingsImpl] with ExtensionIdProvider {

  override def lookup: KafkaSettings.type = KafkaSettings

  override def createExtension(system: ExtendedActorSystem) =
    new KafkaSettingsImpl(system)

}

class KafkaSettingsImpl(system: ExtendedActorSystem) extends Extension {

  private val config = system.settings.config.getConfig("kafka")

  private def toMap(config: Config): Map[String, String] = {
    val map = mutable.Map[String, String]()
    config.entrySet.forEach(e => map.addOne(e.getKey, config.getString(e.getKey)))
    map.toMap
  }

  private val configAsMap = toMap(config)

  val properties = {
    val properties = new Properties()
    configAsMap.foreach {
      case (k, v) => properties.put(k, v)
    }
    properties
  }

  def getProducerSettings: ProducerSettings[String, String] = {
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProperties(configAsMap)
  }

  def getBaseConsumerSettings: ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer )
      .withProperties(configAsMap)
  }

}
