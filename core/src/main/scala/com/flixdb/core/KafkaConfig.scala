package com.flixdb.core

import java.util.Properties

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.mutable

object KafkaConfig extends ExtensionId[KafkaConfigImpl] with ExtensionIdProvider {

  override def lookup: KafkaConfig.type = KafkaConfig

  override def createExtension(system: ExtendedActorSystem) =
    new KafkaConfigImpl(system)

}

class KafkaConfigImpl(system: ExtendedActorSystem) extends Extension {

  // This is "extra" config in the sense that there's already a lot of
  // default configuration under akka.kafka
  private val extraConfig = system.settings.config.getConfig("kafka-extra")

  private def toMap(config: Config): Map[String, String] = {
    val map = mutable.Map[String, String]()
    config.entrySet.forEach(e => map.addOne(e.getKey, config.getString(e.getKey)))
    map.toMap
  }

  val extraConfigAsMap = toMap(extraConfig)

  val extraConfigAsProperties = {
    val properties = new Properties()
    extraConfigAsMap.foreach {
      case (k, v) => properties.put(k, v)
    }
    properties
  }

  def getProducerSettings: ProducerSettings[String, String] = {
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProperties(extraConfigAsMap)
  }

  def getBaseConsumerSettings: ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withProperties(extraConfigAsMap)
  }

}
