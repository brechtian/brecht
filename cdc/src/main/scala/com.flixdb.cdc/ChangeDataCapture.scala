package com.flixdb.cdc

import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

object ChangeDataCapture {

  def source(instance: PostgreSQLInstance, settings: PgCdcSourceSettings): Source[ChangeSet, NotUsed] =
    Source.fromGraph(new PostgreSQLSourceStage(instance, settings))

  def ackSink(instance: PostgreSQLInstance, settings: PgCdcAckSinkSettings): Sink[AckLogSeqNum, NotUsed] =
    Sink.fromGraph(new PostgreSQLAckSinkStage(instance, settings))

}
