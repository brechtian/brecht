package com.flixdb.core

import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Keep
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.util.Timeout
import com.flixdb.cdc._
import com.flixdb.core.CdcActor.{ObservedChange, Start}
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
    ClusterSingletonProxy.props(
      singletonManagerPath = "/user/cdc-to-kafka",
      settings = ClusterSingletonProxySettings(system)),
    name = "cdc-to-kafka-proxy")

  val isStreamRunning: Future[Boolean] = {
    import system.dispatcher
    import scala.concurrent.duration._
    import akka.pattern.ask
    implicit val timeout = Timeout(3.second)
    (proxy ? IsStreamRunning.defaultInstance).mapTo[StreamRunning]
      .map(_.running)
  }

}

object CdcActor {

  case object Start

  case class ObservedChange(changeType: String, change: Change)

}

class CdcActor extends Actor with ActorLogging {

  implicit val system = context.system

  val flixDbConfiguration = FlixDbConfiguration(system)
  val producerSettings = KafkaSettings(system).getProducerSettings

  val dataSource: HikariDataSource = HikariCP(system).getPool("postgresql-cdc-pool")
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
    }
    .viaMat(KillSwitches.single)(Keep.right)
    .to(sink)

  var streamKillSwitch: UniqueKillSwitch = null

  var isRunning: Boolean = false

  override def preStart(): Unit =
    self ! Start

  def stop(): Unit = {
    Option(streamKillSwitch).foreach {
      log.info("Shutting down stream")
      killSwitch => killSwitch.shutdown()
    }
    log.info("Shutting down HikariCP data source")
    dataSource.close()
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
