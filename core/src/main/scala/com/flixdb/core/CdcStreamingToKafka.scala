package com.flixdb.core

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.util.Timeout
import com.flixdb.cdc._
import com.flixdb.core.protobuf.CdcActor.{End, IsStreamRunning, StreamRunning}
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s._

import scala.concurrent.Future

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

  private val proxy = system.actorOf(
    ClusterSingletonProxy
      .props(singletonManagerPath = "/user/cdc-to-kafka", settings = ClusterSingletonProxySettings(system)),
    name = "cdc-to-kafka-proxy"
  )

  val isStreamRunning: Future[Boolean] = {
    import akka.pattern.ask
    import system.dispatcher

    import scala.concurrent.duration._
    implicit val timeout = Timeout(3.second)
    (proxy ? IsStreamRunning.defaultInstance)
      .mapTo[StreamRunning]
      .map(_.running)
  }

}

object CdcActor {

  case object Start

  private def eventEnvelopeFromMap(data: Map[String, String]): EventEnvelope = {

    val stream = data("stream")
    val subStreamId = data("substream_id")

    val eventEnvelope = EventEnvelope(
      eventId = data("event_id"),
      subStreamId = subStreamId,
      eventType = data("event_type"),
      sequenceNum = data("sequence_num").toInt,
      timestamp = data("tstamp").toLong,
      data = data("data"),
      stream = stream,
      tags = data("tags").stripPrefix("{").stripSuffix("}").split(",").toList,
      snapshot = data("snapshot").toBoolean
    )

    eventEnvelope

  }
}

class CdcActor extends Actor with ActorLogging {

  import CdcActor._

  implicit val system = context.system

  implicit val formats = DefaultFormats

  val flixDbConfiguration = FlixDbConfiguration(system)

  val dataSource: HikariDataSource = HikariCP(system).getPool("postgresql-cdc-pool")

  val slotName = "flixdb"

  val kafkaProducerFlow: Flow[ProducerMessage.Envelope[String, String, RowInserted], ProducerMessage.Results[String, String, RowInserted], NotUsed] = {
    val producerSettings = KafkaSettings(system).getProducerSettings
    Producer.flexiFlow[String, String, RowInserted](producerSettings)
  }

  val ackFlow: Flow[(Done, AckLogSeqNum), Done, NotUsed] = ChangeDataCapture.ackFlow[Done](dataSource, PgCdcAckSettings(slotName))

  val stream = ChangeDataCapture
    .source(
      dataSource,
      PgCdcSourceSettings(slotName = "scalatest")
        .withMode(Modes.Peek)
        .withSlotName(slotName)
        .withCreateSlotOnStart(true)
    )
    .mapConcat(_.changes)
    .collect {
      case rowInserted: RowInserted if rowInserted.tableName.endsWith("_events") =>
        val namespace: String = rowInserted.tableName.split("_").dropRight(1).mkString

        log.debug("Captured append to table {} (namespace {})", rowInserted.toString, namespace)

        val data = rowInserted.data
        val eventEnvelope = eventEnvelopeFromMap(data)

        val partition: Int = Math.abs(s"${data("substream_id")}".hashCode % 60)
        val value = eventEnvelope.data
        val topic = s"$namespace-${data("stream")}"
        val key = eventEnvelope.eventType

        log.debug("Writing {} -> {} to partition {} of topic {}", key, value, partition, topic)

        ProducerMessage.single(
          record = new ProducerRecord[String, String](topic, partition, key, value),
          passThrough = rowInserted
        )
    }
    .via(kafkaProducerFlow)
    .map(_.passThrough)
    .map((t: RowInserted) => {
      log.debug("Wrote message to Kafka")
      (Done, AckLogSeqNum(t.commitLogSeqNum))
    })
    .via(ackFlow)
    .viaMat(KillSwitches.single)(Keep.right)
    .to(Sink.ignore)


  var streamKillSwitch: UniqueKillSwitch = null

  var isRunning: Boolean = false

  override def preStart(): Unit =
    self ! Start

  override def aroundPostStop(): Unit = {
    log.info("Shutting down data source")
    dataSource.close()
  }

  def stop(): Unit = {
    Option(streamKillSwitch).foreach {
      log.info("Shutting down stream")
      killSwitch => killSwitch.shutdown()
    }
  }

  override def receive: Receive = {
    case Start =>
      log.info("Started to stream changes from PostgreSQL to Kafka")
      streamKillSwitch = stream.run()
      isRunning = true
    case End =>
      log.info("Received termination message")
      stop()
    case m: IsStreamRunning =>
      sender() ! StreamRunning(isRunning)
  }

}
