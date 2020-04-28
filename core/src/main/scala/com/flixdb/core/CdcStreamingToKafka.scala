package com.flixdb.core

import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.kafka.scaladsl.Producer
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.Keep
import com.flixdb.cdc._
import com.flixdb.core.CdcActor.{ObservedChange, Start}
import com.flixdb.core.protobuf.CdcActor.End
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s._

object CdcStreamingToKafka extends ExtensionId[CdcStreamingToKafkaImpl] with ExtensionIdProvider {

  override def lookup: CdcStreamingToKafka.type = CdcStreamingToKafka

  override def createExtension(system: ExtendedActorSystem) =
    new CdcStreamingToKafkaImpl(system)

}

class CdcStreamingToKafkaImpl(system: ExtendedActorSystem) extends Extension {

  // start singleton actor

  private val cdcToKafkaSingletonManager: ActorRef =
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props(classOf[CdcActor]),
        settings = ClusterSingletonManagerSettings(system),
        terminationMessage = End
      ),
      name = "cdc-to-kafka"
    )

}

object CdcActor {

  case object Start

  case class ObservedChange(changeType: String, change: Change)

}

class CdcActor extends Actor with ActorLogging {

  implicit val system = context.system

  val flixDbConfiguration = FlixDbConfiguration(system)
  val producerSettings = KafkaSettings(system).getProducerSettings

  val dataSource: HikariDataSource = HikariCP(system).getPool("postgres-cdc")
  val topic = flixDbConfiguration.cdcKafkaStreamName
  val sink = Producer.plainSink(producerSettings)
  val stream = ChangeDataCapture
    .restartSource(
      dataSource,
      PgCdcSourceSettings()
        .withMode(Modes.Get) // TODO: change to Modes.Peek (at least once delivery) and add an AckSink
        .withSlotName("cdc")
        .withCreateSlotOnStart(true)
    )
    .mapConcat(_.changes)
    .collect {
      case change: Change =>
        import org.json4s.jackson.Serialization._
        implicit val formats = DefaultFormats
        val changeType = change match {
          case ri: RowInserted => "RowInserted"
          case ru: RowUpdated  => "RowUpdated"
          case rd: RowDeleted  => "RowDeleted"
        }
        val value = writePretty(ObservedChange(changeType, change))
        log.info("Captured change \n{}\n", value)
        new ProducerRecord[String, String](topic, value)
    }.viaMat(KillSwitches.single)(Keep.right).to(sink)

  var streamKillSwitch: UniqueKillSwitch = null

  override def preStart(): Unit =
    self ! Start

  def stop(): Unit = {
    log.info("Shutting down stream")
    Option(streamKillSwitch).foreach(_.shutdown())
    log.info("Shutting down HikariCP data source")
    dataSource.close()
  }

  override def receive: Receive = {
    case Start =>
      log.info("Started to stream changes from PostgreSQL to Kafka")
      streamKillSwitch = stream.run()
    case End =>
      log.info("Received termination message")
      stop()
  }

}
