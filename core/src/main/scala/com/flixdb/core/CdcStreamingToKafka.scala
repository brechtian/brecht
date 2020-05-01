package com.flixdb.core

import akka._
import akka.actor._
import akka.cluster.singleton._
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream._
import akka.stream.scaladsl._
import akka.util.Timeout
import com.flixdb.cdc._
import com.flixdb.core.KafkaEventEnvelope._
import com.flixdb.core.protobuf.CdcActor._
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.producer.ProducerRecord

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

  case class StreamCompleted(ex: Option[Exception])

  private def mapToKafkaEventEnvelope(data: Map[String, String]): KafkaEventEnvelope = {
    val stream = data("stream")
    val subStreamId = data("substream_id")
    val eventEnvelope = KafkaEventEnvelope(
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
  import spray.json._

  implicit val system = context.system

  val flixDbConfiguration = FlixDbConfiguration(system)

  def getDataSource: HikariDataSource = HikariCP(system).getPool("postgresql-cdc-pool")

  val slotName = "flixdb"

  val kafkaProducerFlow: Flow[
    ProducerMessage.Envelope[String, String, RowInserted],
    ProducerMessage.Results[String, String, RowInserted],
    NotUsed
  ] = {
    val producerSettings = KafkaSettings(system).getProducerSettings
    Producer.flexiFlow[String, String, RowInserted](producerSettings)
  }

  def ackFlow(dataSource: HikariDataSource): Flow[(Done, AckLogSeqNum), Done, NotUsed] =
    ChangeDataCapture.ackFlow[Done](dataSource, PgCdcAckSettings(slotName))

  def stream(dataSource: HikariDataSource) =
    ChangeDataCapture
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
          val eventEnvelope = mapToKafkaEventEnvelope(data)
          val value = eventEnvelope.toJson.compactPrint
          val topic = s"$namespace-${data("stream")}"
          val key = s"${data("substream_id")}"
          log.debug("Writing key {} to topic {}", key, value, topic)
          ProducerMessage.single(
            record = new ProducerRecord[String, String](topic, key, value),
            passThrough = rowInserted
          )
      }
      .via(kafkaProducerFlow)
      .map(_.passThrough)
      .map((t: RowInserted) => {
        log.debug("Wrote message to Kafka")
        (Done, AckLogSeqNum(t.commitLogSeqNum))
      })
      .via(ackFlow(dataSource))
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)

  var streamKillSwitch: UniqueKillSwitch = null
  var streamFut: Future[Done] = null

  override def preStart(): Unit =
    self ! Start

  def stop(): Unit = {
    Option(streamKillSwitch).foreach {
      log.info("Shutting down stream")
      killSwitch => killSwitch.shutdown()
    }
  }

  var isEnding = false

  override def receive: Receive = {
    case Start =>
      log.info("Started to stream changes from PostgreSQL to Kafka")
      val dataSource = getDataSource
      val values = stream(dataSource).run()
      streamKillSwitch = values._1
      streamFut = values._2
      import system.dispatcher
      streamFut.map(_ => dataSource.close())
    case End =>
      isEnding = true
      log.info("Received termination message")
      stop()
    case m: IsStreamRunning =>
      val running = (!streamFut.isCompleted)
      sender() ! StreamRunning(running)
  }

}
