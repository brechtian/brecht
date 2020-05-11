package com.flixdb.core

import akka._
import akka.actor._
import akka.cluster.singleton._
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl._
import akka.util.Timeout
import com.flixdb.cdc.{AckLogSeqNum, Change, Modes, PgCdcAckSettings, PgCdcSourceSettings, PostgreSQLInstance, RowInserted}
import com.flixdb.cdc.scaladsl._
import com.flixdb.core.KafkaEventEnvelope._
import com.flixdb.core.protobuf.CdcActor._
import org.apache.kafka.clients.producer.ProducerRecord
import spray.json._

import scala.concurrent.Future
import scala.util.{Failure, Success}

object CdcStreamingToKafka extends ExtensionId[CdcStreamingToKafkaImpl] with ExtensionIdProvider {

  override def lookup: CdcStreamingToKafka.type = CdcStreamingToKafka

  override def createExtension(system: ExtendedActorSystem) =
    new CdcStreamingToKafkaImpl(system)

}

class CdcStreamingToKafkaImpl(system: ExtendedActorSystem) extends Extension {

  import system.dispatcher

  // start singleton actor

  private val cdcToKafkaSingletonManager: ActorRef =
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props(classOf[CdcStreamingToKafkaActor]),
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

  def isStreamRunning: Future[Boolean] = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(3.second)
    (proxy ? IsStreamRunning.defaultInstance)
      .mapTo[StreamRunning]
      .map(_.running)
  }

}

class CdcStreamingToKafkaActor extends Actor with ActorLogging {

  import CdcStreamingToKafkaActor._

  implicit val system = context.system

  val flixDbConfig = FlixDbConfig(system)

  var isRunning = true

  val dataSource = HikariCP(system).startHikariDataSource("postgresql-cdc-pool")

  val changeDataCapture = ChangeDataCapture(PostgreSQLInstance(dataSource))

  val buildKafkaMessage = Flow[Change].collect {
    case rowInserted: RowInserted if isEventsTable(rowInserted.tableName) =>
      val namespace: String = extractNamespace(rowInserted.tableName)
      log.debug("Captured append to table {} (namespace {})", rowInserted.toString, namespace)
      val data = rowInserted.data
      val eventEnvelope = convertToKafkaEventEnvelope(data)
      val value = eventEnvelope.toJson.compactPrint
      val topic = flixDbConfig.getTopicName(namespace, eventEnvelope.stream)
      val key = eventEnvelope.subStreamId
      log.debug("Attempting to write message with key {} to topic {}", key, value, topic)
      ProducerMessage.single(
        record = new ProducerRecord[String, String](topic, key, value),
        passThrough = rowInserted
      )
  }

  val postToKafka = {
    val producerSettings = KafkaConfig(system).getProducerSettings
    Producer.flexiFlow[String, String, RowInserted](producerSettings)
  }

  val cdcSource = changeDataCapture.source(
    PgCdcSourceSettings(slotName = SlotName)
      .withMode(Modes.Peek)
      .withCreateSlotOnStart(true)
      .withDropSlotOnFinish(false)
      .withCloseDataSourceOnFinish(true)
  )

  val cdcAckFlow = changeDataCapture.ackFlow[Done](PgCdcAckSettings(SlotName))

  val cdcToKafkaSource = cdcSource
    .mapConcat(_.changes)
    .via(buildKafkaMessage)
    .via(postToKafka)
    .map(_.passThrough)
    .map((t: RowInserted) => {
      (Done, AckLogSeqNum(t.commitLogSeqNum))
    })
    .via(cdcAckFlow)

  val streamKillSwitch =
    cdcToKafkaSource
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.onComplete {
        case Failure(exception) =>
          isRunning = false
          log.error(exception, "Stream completed with failure")
          self ! StreamFailed(exception)
        case Success(_) =>
          log.info("Stream completed succesfully")
          isRunning = false
      })
      .run()

  override def preStart(): Unit =
    self ! Start

  def stop(): Unit = {
    Option(streamKillSwitch).foreach {
      log.info("Shutting down stream")
      killSwitch => killSwitch.shutdown()
    }
  }

  override def receive: Receive = {
    case Start =>
      log.info("Started to stream changes from PostgreSQL to Kafka")
    case End =>
      log.info("Received termination message")
      stop()
    case StreamFailed(ex) =>
      throw ex // this will restart the actor, possibly on another node
    case _: IsStreamRunning =>
      sender() ! StreamRunning(isRunning)
  }

}

object CdcStreamingToKafkaActor {

  case object Start

  case class StreamFailed(ex: Throwable)

  val SlotName: String = "flixdb"

  def isEventsTable(tableName: String): Boolean =
    tableName.endsWith("_events")

  def extractNamespace(tableName: String): String = {
    val namespace: String = tableName.split("_").dropRight(1).mkString
    namespace
  }

  def convertToKafkaEventEnvelope(data: Map[String, String]): KafkaEventEnvelope = {
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
