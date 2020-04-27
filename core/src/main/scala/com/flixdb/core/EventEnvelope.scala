package com.flixdb.core

/**
  * @param eventId UUID for this event
  * @param subStreamId Id of the domain entity (e.g., account-123)
  * @param eventType FQN of the event class
  * @param sequenceNum Sequence number of the event (per entity)
  * @param data JSON data (actual event payload)
  * @param stream Name of the stream that the event belongs to (e.g., account)
  * @param tags Other streams that the event belongs to
  * @param timestamp the number of milliseconds since January 1, 1970, 00:00:00 GMT.
  */
final case class EventEnvelope(
    eventId: String,
    subStreamId: String,
    eventType: String,
    sequenceNum: Int,
    data: String,
    stream: String,
    tags: List[String],
    timestamp: Long
)
