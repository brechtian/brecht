package com.flixdb.core

import java.util.UUID.randomUUID

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import com.flixdb.core.protobuf.PublishMsgs

class TestEntityActor extends AnyFunSuiteLike with BeforeAndAfterAll with Matchers {

  test("Test deduplication") {

    val existing =
      EventEnvelope(
        eventId = "a4157b56-a915-4f1c-95c9-3907f47c9d0e",
        entityId = "account-42",
        eventType = "com.megacorp.AccountCreated",
        sequenceNum = 0,
        data = """{"owner": 42}""",
        stream = "accounts",
        tags = List("users"),
        timestamp = Long.MinValue
      ) :: Nil

    val event1 = PublishMsgs.PbEventEnvelope.defaultInstance
      .withSequenceNum(0)
      .withData("""{"owner": 42}""")
      .withEventId("a4157b56-a915-4f1c-95c9-3907f47c9d0e")
      .withEventType("com.megacorp.AccountCreated")
      .withTags(List("users"))

    val event2 = PublishMsgs.PbEventEnvelope.defaultInstance
      .withSequenceNum(1)
      .withData("""{"owner": 42}""")
      .withEventId("431b2c79-6d10-4de8-b420-795d8d6f79ef")
      .withEventType("com.megacorp.AccountSuspended")
      .withTags(List("users"))

    val request = PublishMsgs.PbPublishEventsRequest.defaultInstance
      .withNamespace("default")
      .withStream("accounts")
      .withEntityId("account-42")
      .withEventEnvelopes(List(event1, event2))

    val result = EntityActor.deduplicate(existing, request)

    result.eventEnvelopes.size shouldBe 1
    result.eventEnvelopes.head.eventId shouldBe "431b2c79-6d10-4de8-b420-795d8d6f79ef"

  }

  test("Event envelopes conversion from Protocol Buffers") {

    val event1 = PublishMsgs.PbEventEnvelope.defaultInstance
      .withSequenceNum(1)
      .withData("""{"owner": 42}""")
      .withEventId(randomUUID().toString)
      .withEventType("com.megacorp.AccountCreated")
      .withTags(List("accounts", "users"))

    val result = EntityActor.fromProtobuf(
      stream = "accounts",
      entityId = "42",
      timestamp = Long.MaxValue,
      event1 :: Nil
    )

    result shouldBe an[List[EventEnvelope]]

    val element: EventEnvelope = result.head

    element.sequenceNum shouldBe event1.sequenceNum
    element.data shouldBe event1.data
    element.eventId shouldBe event1.eventId
    element.eventType shouldBe event1.eventType
    element.tags shouldBe event1.tags.toList
    element.stream shouldBe "accounts"
    element.entityId shouldBe "42"
    element.timestamp shouldBe Long.MaxValue

  }

  test("Event envelopes conversion to Protocol Buffers") {

    val event1: EventEnvelope = EventEnvelope(
      eventId = randomUUID().toString,
      entityId = s"account-1",
      eventType = "com.megacorp.AccountCreated",
      sequenceNum = 0,
      data = """{"owner": "John Smith"}""",
      stream = "accounts",
      tags = List("megacorp"),
      timestamp = 42L
    )

    val result = EntityActor.toProtobuf(event1 :: Nil)
    result shouldBe an[List[PublishMsgs.PbEventEnvelope]]
    val element = result.head

    element.eventId shouldBe event1.eventId
    element.entityId shouldBe event1.entityId
    element.eventType shouldBe event1.eventType
    element.data shouldBe event1.data
    element.sequenceNum shouldBe event1.sequenceNum
    element.stream shouldBe event1.stream
    element.tags shouldBe event1.tags
    element.timestamp shouldBe event1.timestamp

  }

  test("Sequence number range validation (1)") {

    def randomId = java.util.UUID.randomUUID().toString

    val existingEvents = (0 to 1)
      .map(i =>
        EventEnvelope(
          eventId = randomId,
          entityId = "account-42",
          eventType = "MoneyIn",
          sequenceNum = i,
          data = """{"amount":888}""",
          stream = "accounts",
          tags = Nil,
          timestamp = -1
        )
      )
      .toList

    val newEvents = (2 to 4).map(i => PublishMsgs.PbEventEnvelope.defaultInstance.withSequenceNum(i))

    EntityActor.validateSeqNumberRange(
      existingEvents,
      PublishMsgs.PbPublishEventsRequest.defaultInstance
        .withNamespace("default")
        .withEntityId("42")
        .withStream("accounts")
        .withEventEnvelopes(newEvents)
    ) shouldBe true

    // correct: sequence numbers are from 0 to 4

  }

  test("Sequence number range validation (2)") {

    def randomId = java.util.UUID.randomUUID().toString

    val existingEvents = (0 to 0)
      .map(i =>
        EventEnvelope(
          eventId = randomId,
          entityId = "account-42",
          eventType = "MoneyIn",
          sequenceNum = i,
          data = """{"amount":888}""",
          stream = "accounts",
          tags = Nil,
          timestamp = -1
        )
      )
      .toList

    val newEvents = (3 to 5).map(i => PublishMsgs.PbEventEnvelope.defaultInstance.withSequenceNum(i))

    EntityActor.validateSeqNumberRange(
      existingEvents,
      PublishMsgs.PbPublishEventsRequest.defaultInstance
        .withNamespace("default")
        .withEntityId("42")
        .withStream("accounts")
        .withEventEnvelopes(newEvents)
    ) shouldBe false

    // not valid because sequence numbers 1 and 2 are missing

  }

  test("Sequence number validation (3)") {

    EntityActor.validateSeqNumberRange(
      eventEnvelopes = Nil,
      PublishMsgs.PbPublishEventsRequest.defaultInstance
        .withNamespace("default")
        .withEntityId("42")
        .withStream("accounts")
        .withEventEnvelopes(Nil)
    ) shouldBe true

  }

  test("Sequence number validation (4)") {

    val existingEvents = Nil

    val newEvents = (0 to 5).map(i => PublishMsgs.PbEventEnvelope.defaultInstance.withSequenceNum(i))

    EntityActor.validateSeqNumberRange(
      existingEvents,
      PublishMsgs.PbPublishEventsRequest.defaultInstance
        .withNamespace("default")
        .withEntityId("42")
        .withStream("accounts")
        .withEventEnvelopes(newEvents)
    ) shouldBe true

    // sequence numbers from 0 to 5

  }

  test("Sequence number validation (5)") {

    val existingEvents = Nil

    val newEvents = (1 to 5).map(i => PublishMsgs.PbEventEnvelope.defaultInstance.withSequenceNum(i))

    EntityActor.validateSeqNumberRange(
      existingEvents,
      PublishMsgs.PbPublishEventsRequest.defaultInstance
        .withNamespace("default")
        .withEntityId("42")
        .withStream("accounts")
        .withEventEnvelopes(newEvents)
    ) shouldBe false

    // not valid because first sequence number should be 0

  }

  test("Sequence number validation (6)") {

    def randomId = java.util.UUID.randomUUID().toString

    val existingEvents = (0 to 1)
      .map(i =>
        EventEnvelope(
          eventId = randomId,
          entityId = "account-42",
          eventType = "MoneyIn",
          sequenceNum = i,
          data = """{"amount":888}""",
          stream = "accounts",
          tags = Nil,
          timestamp = -1
        )
      )
      .toList

    val newEvents = Nil

    EntityActor.validateSeqNumberRange(
      existingEvents,
      PublishMsgs.PbPublishEventsRequest.defaultInstance
        .withNamespace("default")
        .withEntityId("42")
        .withStream("accounts")
        .withEventEnvelopes(newEvents)
    ) shouldBe true

  }

}
