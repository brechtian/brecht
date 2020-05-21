package com.flixdb.core

import akka.Done
import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream._
import akka.stream.scaladsl._
import akka.util.Timeout
import com.flixdb.cdc._
import com.flixdb.cdc.scaladsl._
import com.flixdb.core.KafkaEventEnvelope._
import com.flixdb.core.KafkaMigrator.{End, IsStreamRunning}
import io.prometheus.client.Gauge
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object KafkaMigration {

  def apply(changeDataCapture: ChangeDataCapture)(implicit system: ActorSystem[_]): KafkaMigration =
    new KafkaMigration(changeDataCapture)(system)

}

class KafkaMigration(changeDataCapture: ChangeDataCapture)(system: ActorSystem[_]) {

  private val logger = LoggerFactory.getLogger(classOf[KafkaMigration])

  private implicit val actorSystem = system

  private implicit val ec = system.executionContext

  // start singleton actor
  private val singletonManager = ClusterSingleton(system)

  val singletonProxy: ActorRef[KafkaMigrator.KafkaMigratorCommand] = singletonManager.init(
    singleton = SingletonActor(
      behavior = Behaviors
        .supervise(KafkaMigrator.apply(changeDataCapture))
        .onFailure[Exception](SupervisorStrategy.restartWithBackoff(2.second, 10.seconds, 0.2)),
      name = "kafka-migrator"
    ).withStopMessage(End)
  )

  def isStreamRunning: Future[Boolean] = {
    implicit val timeout = Timeout(3.second)
    import akka.actor.typed.scaladsl.AskPattern._
    val result: Future[Boolean] = (singletonProxy.ask[Boolean](ref => IsStreamRunning(ref)))
    result
  }

}

object KafkaMigrator {

  sealed trait KafkaMigratorCommand

  case class StreamFailed(ex: Throwable) extends KafkaMigratorCommand
  case class IsStreamRunning(replyTo: ActorRef[Boolean]) extends KafkaMigratorCommand
  case object End extends KafkaMigratorCommand

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

  val runningStreams = Gauge
    .build()
    .help("Total number of streams migrating events to Kafka (should NOT be more than one).")
    .name("kafka_migration_streams_total")
    .register()

  class Stream(changeDataCapture: ChangeDataCapture)(onFail: Throwable => Unit)(implicit system: ActorSystem[_]) {
    runningStreams.inc()

    private var isRunning: Boolean = true

    private val log = LoggerFactory.getLogger(classOf[Stream])

    private val SlotName: String = "flixdb"

    private val flixDbConfig = FlixDbConfig(system)

    private val kafkaConfig = KafkaConfig(system)

    def newProducerRecord(topic: String, key: String, value: String) = {
      log.debug("Getting ready to write message with key {} to topic {}", key, value, topic)
      new ProducerRecord[String, String](topic, key, value)
    }

    private val buildKafkaMessage = Flow[Change].collect {
      case rowInserted: RowInserted if isEventsTable(rowInserted.tableName) =>
        val namespace: String = extractNamespace(rowInserted.tableName)
        log.debug("Captured append to table {} (namespace {})", rowInserted.toString, namespace)
        val data = rowInserted.data
        val eventEnvelope = convertToKafkaEventEnvelope(data)
        val value = eventEnvelope.toJson.compactPrint

        val mainRecord = {
          val topic = flixDbConfig.getTopicName(namespace, eventEnvelope.stream)
          val key = eventEnvelope.subStreamId
          newProducerRecord(topic, key, value)
        }

        val otherRecords = {
          eventEnvelope.tags.map((tag: String) => {
            val topic = flixDbConfig.getTopicNameForTag(namespace, tag)
            val key = namespace
            newProducerRecord(topic, key, value)
          })
        }

        ProducerMessage.multi(
          records = mainRecord :: otherRecords,
          passThrough = rowInserted
        )
    }

    private val postToKafka = {
      val producerSettings = kafkaConfig.getProducerSettings
      Producer.flexiFlow[String, String, RowInserted](producerSettings)
    }

    private val cdcSource = changeDataCapture.source(
      PgCdcSourceSettings(slotName = SlotName)
        .withMode(Modes.Peek)
        .withCreateSlotOnStart(true)
        .withDropSlotOnFinish(false)
        .withCloseDataSourceOnFinish(true)
    )

    private val cdcAckFlow = changeDataCapture.ackFlow[Done](PgCdcAckSettings(SlotName))

    private val cdcToKafkaSource = cdcSource
      .mapConcat(_.changes)
      .via(buildKafkaMessage)
      .via(postToKafka)
      .map(_.passThrough)
      .map((t: RowInserted) => {
        (Done, AckLogSeqNum(t.commitLogSeqNum))
      })
      .via(cdcAckFlow)

    log.info("Starting")
    val streamKillSwitch =
      cdcToKafkaSource
        .viaMat(KillSwitches.single)(Keep.right)
        .to(Sink.onComplete {
          case Failure(exception) =>
            log.error("Stream completed with failure", exception)
            isRunning = false
            runningStreams.dec()
            onFail(exception)
          case Success(_) =>
            log.info("Stream completed succesfully")
            isRunning = false
            runningStreams.dec()
        })
        .run()

    def getIsRunning: Boolean = isRunning
  }

  def apply(changeDataCapture: ChangeDataCapture): Behavior[KafkaMigratorCommand] = Behaviors.setup { context =>
    context.log.info("Starting")
    implicit val system = context.system
    val stream = new Stream(changeDataCapture)(onFail = (t: Throwable) => {
      context.self.tell(StreamFailed(t))
    })

    Behaviors.receiveMessage {
      case End =>
        context.log.info("Received termination message")
        Option(stream.streamKillSwitch).foreach {
          context.log.info("Shutting down stream")
          killSwitch => killSwitch.shutdown()
        }
        Behaviors.same
      case IsStreamRunning(replyTo) =>
        replyTo ! stream.getIsRunning
        Behaviors.same
      case StreamFailed(ex: Throwable) =>
        throw ex
        Behaviors.same
    }
  }

}
