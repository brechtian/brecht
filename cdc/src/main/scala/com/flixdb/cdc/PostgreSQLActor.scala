package com.flixdb.cdc

import java.io.Closeable

import akka.Done
import akka.actor.ActorSystem
import akka.actor.ActorSystem.Settings
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, MailboxSelector, Props}
import akka.dispatch.{PriorityGenerator, UnboundedStablePriorityMailbox}
import com.flixdb.cdc.PostgreSQLActor.{Flush, PostgreSQLActorMessage}
import com.typesafe.config.Config
import javax.sql.DataSource

import scala.concurrent.duration.FiniteDuration

class PostgreSQLInstance {

  private[cdc] var postgreSQLActor: ActorRef[PostgreSQLActorMessage] = null

  def this(dataSource: DataSource with Closeable)(implicit actorSystem: ActorSystem) = {
    this()
    postgreSQLActor = actorSystem.spawn(
      PostgreSQLActor.apply(new PostgreSQL(dataSource)),
      name = PostgreSQLActor.name,
      props = PostgreSQLActor.props
    )
  }

  def this(postgreSQLActor: ActorRef[PostgreSQLActorMessage]) = {
    this()
    this.postgreSQLActor = postgreSQLActor
  }

}

object PostgreSQLInstance {

  def apply(dataSource: DataSource with Closeable)(implicit actorSystem: ActorSystem) = {
    new PostgreSQLInstance(dataSource)(actorSystem)
  }

  def apply(postgreSQLActor: ActorRef[PostgreSQLActorMessage]) = {
    new PostgreSQLInstance(postgreSQLActor)
  }

}

object PostgreSQLActor {

  def name: String = s"cdc-${java.util.UUID.randomUUID().toString.take(8)}"

  def props: Props =
    MailboxSelector
      .fromConfig("cdc.mailbox")
      .withDispatcherFromConfig("cdc.blocking-io-dispatcher")

  sealed trait PostgreSQLActorMessage

  case class Start(slotName: String, pluginName: String, createSlotIfNotExists: Boolean, replyTo: ActorRef[Done])
      extends PostgreSQLActorMessage

  case class GetChanges(
      slotName: String,
      mode: Mode,
      plugin: Plugin,
      maxItems: Int,
      columnsToIgnore: Map[String, List[String]],
      replyTo: ActorRef[ChangeSetList]
  ) extends PostgreSQLActorMessage

  case class Stop(delay: Option[FiniteDuration], slotName: String, dropSlot: Boolean, closeDataSource: Boolean)
      extends PostgreSQLActorMessage

  case class Flush(slotName: String, logSeqNum: String, replyTo: ActorRef[Done]) extends PostgreSQLActorMessage

  case class ChangeSetList(list: List[ChangeSet])

  def apply(postgreSQL: PostgreSQL): Behavior[PostgreSQLActorMessage] = Behaviors.withTimers {
    timer: TimerScheduler[PostgreSQLActorMessage] =>
      Behaviors.receive { (_, message) =>
        {
          message match {
            case Start(slotName, pluginName, createSlotIfNotExists, replyTo) =>
              if (!postgreSQL.slotExists(slotName, pluginName))
                if (createSlotIfNotExists)
                  postgreSQL.createSlot(slotName, pluginName)
              replyTo ! Done
              Behaviors.same
            case GetChanges(slotName, mode, plugin, maxItems, columnsToIgnore, sender) =>
              val result: List[ChangeSet] = {
                val slotChanges: List[PostgreSQL.SlotChange] =
                  postgreSQL.pullChanges(mode, slotName, maxItems)
                plugin match {
                  case Plugins.TestDecoding =>
                    TestDecodingPlugin.transformSlotChanges(slotChanges, columnsToIgnore)
                  case Plugins.Wal2Json =>
                    Wal2JsonPlugin.transformSlotChanges(slotChanges, columnsToIgnore)
                }
              }
              sender ! ChangeSetList(result)
              Behaviors.same
            case Flush(slotName, logSeqNum, sender) =>
              postgreSQL.flush(slotName, logSeqNum)
              sender ! Done
              Behaviors.same
            case Stop(None, slotName, dropSlot, closeDataSource) =>
              if (dropSlot)
                postgreSQL.dropSlot(slotName)
              if (closeDataSource)
                postgreSQL.ds.close()
              Behaviors.stopped
            case s @ Stop(Some(delay), _, _, _) =>
              val msg = s.copy(delay = None)
              timer.startSingleTimer("TickKey", msg, delay)
              Behaviors.same
          }
        }
      }

  }
}

class PriorityMailbox(settings: Settings, config: Config)
    extends UnboundedStablePriorityMailbox(
      // Create a new PriorityGenerator, lower priority means more important
      PriorityGenerator {
        case _: Flush  => 0
        case otherwise => 1
      }
    )
