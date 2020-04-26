package com.flixdb.core

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import com.flixdb.core.CrdtDeduplicatorActor.{Check, CheckResult, Inform}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class TestCrdtDeduplicatorActor
    extends TestKit(ActorSystem("flixdb"))
    with AnyFunSuiteLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually
    with Matchers
    with IntegrationPatience {

  val crdtDedupActor =
    system.actorOf(Props(new CrdtDeduplicatorActor), "deduplicator")

  override def afterAll: Unit = {
    crdtDedupActor ! PoisonPill
    TestKit.shutdownActorSystem(system)
  }

  test("Can check for duplicate ids") {
    implicit val timeout = Timeout(2.seconds)
    crdtDedupActor ! Inform("c5405bcc-9552-43d0-b061-88464263167d" :: Nil)
    crdtDedupActor ! Inform("6d606129-9efa-4701-83bf-a8fd42390c98" :: Nil)
    crdtDedupActor ! Inform("6c1c326a-4982-4573-bc20-b63dfdb7edef" :: Nil)

    eventually {
      (crdtDedupActor ? Check("6c1c326a-4982-4573-bc20-b63dfdb7edef" :: Nil))
        .mapTo[CheckResult]
        .futureValue shouldBe CheckResult(Set("6c1c326a-4982-4573-bc20-b63dfdb7edef"), Set.empty)
    }

    (crdtDedupActor ? Check("c78f75ce-1ca4-48a9-a824-90ea4fa680e4" :: Nil))
      .mapTo[CheckResult]
      .futureValue shouldBe CheckResult(Set.empty, Set("c78f75ce-1ca4-48a9-a824-90ea4fa680e4"))
  }

}
