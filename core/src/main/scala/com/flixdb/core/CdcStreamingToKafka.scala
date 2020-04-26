package com.flixdb.core

import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider,
  Props
}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.kafka.scaladsl.Producer
import com.flixdb.cdc.{ChangeDataCapture, PgCdcSourceSettings, PostgreSQLInstance, RowInserted}
import com.flixdb.core.CdcActor.Start
import org.apache.kafka.clients.producer.ProducerRecord

object CdcStreamingToKafka extends ExtensionId[CdcStreamingToKafkaImpl] with ExtensionIdProvider {

  override def lookup = CdcStreamingToKafka

  override def createExtension(system: ExtendedActorSystem) =
    new CdcStreamingToKafkaImpl(system)

}

class CdcStreamingToKafkaImpl(system: ExtendedActorSystem) extends Extension {

  // start singleton actor

  case object End extends CborSerializable

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
}

class CdcActor extends Actor with ActorLogging {

  implicit val system = context.system

  val dataSource = HikariCP(system).getPool("postgres-cdc")

  val topic = "cdc" // TODO: move to configuration

  val producerSettings = KafkaProducerSettings(system).getSettings

  override def preStart(): Unit =
    self ! Start

  import spray.json.DefaultJsonProtocol._
  import spray.json._

  val sink = Producer.plainSink(producerSettings)

  val stream = ChangeDataCapture
    .source(
      PostgreSQLInstance(dataSource, "cdc"),
      PgCdcSourceSettings() // default settings
    )
    .mapConcat(_.changes)
    .collect {
      case RowInserted(schemaName, tableName, commitLogSeqNum, transactionId, data: Map[String, String], schema) =>
        val stream = data.get("stream")
        val entityId = data.get("entityId")
        log.info("change data capture {} {} ", stream, entityId)
        val value = data.toJson.compactPrint
        new ProducerRecord[String, String](topic, value)
    }
    .to(sink)

  override def receive: Receive = {
    case Start =>
      stream.run()
  }

}
