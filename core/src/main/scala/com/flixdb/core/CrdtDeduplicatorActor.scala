package com.flixdb.core

import akka.actor.{
  Actor,
  ActorLogging,
  ActorSystem,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider,
  Props
}
import akka.cluster.ddata.Replicator.{Changed, Subscribe, UpdateResponse}
import akka.cluster.ddata.{DistributedData, GSet, GSetKey, Replicator}
import akka.event.Logging
import akka.util.Timeout
import com.flixdb.core.CrdtDeduplicatorActor.{Check, CheckResult, Inform}

import scala.concurrent.Future

// NOTE: not used yet since GSet is not quite scalable for what what we want to do

object CrdtDeduplicator extends ExtensionId[CrdtDeduplicatorImpl] with ExtensionIdProvider {

  override def lookup = CrdtDeduplicator

  override def createExtension(system: ExtendedActorSystem) = new CrdtDeduplicatorImpl(system)

}

class CrdtDeduplicatorImpl(system: ActorSystem) extends Extension {

  import akka.pattern.ask
  import system.dispatcher

  import scala.concurrent.duration._

  private val deduplicatorActor = system.actorOf(Props(new CrdtDeduplicatorActor), "deduplicator")

  private implicit val timeout = Timeout(1.seconds)

  private val logger = Logging(system, classOf[CrdtDeduplicatorImpl])

  def check(eventUniqueIds: List[String]): Future[CheckResult] = {
    (deduplicatorActor ? Check(eventUniqueIds)).mapTo[CheckResult]
  }.recoverWith {
    case t =>
      logger.error("Failed to get a response from actor", t)
      Future.successful(CheckResult(Set.empty, dne = eventUniqueIds.toSet))
  }

  def inform(eventUniqueId: String): Unit = {
    deduplicatorActor ! Inform(eventUniqueId :: Nil)
  }

  def inform(eventUniqueIds: List[String]): Unit = {
    deduplicatorActor ! Inform(eventUniqueIds)
  }

}

object CrdtDeduplicatorActor {

  case class Check(eventUniqueIds: List[String])

  case class CheckResult(exists: Set[String], dne: Set[String])

  case class Inform(eventUniqueIds: List[String])
}

class CrdtDeduplicatorActor extends Actor with ActorLogging {

  val DataKey = GSetKey[String]("events") // TODO: we need a set with a maximum size property
  var currentData: Set[String] = null

  val replicator = DistributedData(context.system).replicator
  implicit val node = DistributedData(context.system).selfUniqueAddress

  replicator ! Subscribe(DataKey, self)

  override def receive: Receive = {

    case Inform(eventUniqueIds) =>
      replicator ! Replicator.Update(DataKey, GSet.empty[String], Replicator.WriteLocal) { set: GSet[String] =>
        eventUniqueIds.foldLeft(set)((s, item) => s + item)
      }

    case r: UpdateResponse[_] =>
      log.debug("received update response")

    case Check(eventUniqueIds: List[String]) =>
      val eventUniqueIdsSet = eventUniqueIds.toSet
      Option(currentData) match {
        case Some(data: Set[String]) =>
          sender() ! CheckResult(eventUniqueIdsSet.intersect(data), dne = eventUniqueIdsSet -- (data))
        case _ => sender() ! CheckResult(Set.empty, eventUniqueIdsSet)
      }

    case c @ Changed(DataKey) =>
      log.debug("received changes")
      val data: GSet[String] = c.get(DataKey)
      currentData = data.elements

  }

}
