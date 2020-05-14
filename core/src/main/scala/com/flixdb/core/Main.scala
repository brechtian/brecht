package com.flixdb.core

import akka.actor
import akka.actor.typed._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.flixdb.core.postgresql.PostgreSQLActor
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class Main

object Main extends App with JsonSupport {

  def apply(): Behavior[SpawnProtocol.Command] =
    Behaviors.setup { ctx: ActorContext[SpawnProtocol.Command] =>
      implicit val untypedSystem: actor.ActorSystem = ctx.system.toClassic
      implicit val materializer = ActorMaterializer()(ctx.system.toClassic)
      implicit val ec: ExecutionContextExecutor = ctx.system.executionContext

      val postgreSQLActor: ActorRef[PostgreSQLActor.Request] = PostgreSQLActor.getRouter(ctx)
      val eventStoreController = new EventStoreControllerImpl(postgreSQLActor)

      val httpJsonRoutes = new HttpJsonRoutes(eventStoreController)

      val serverBinding: Future[Http.ServerBinding] =
        Http()(untypedSystem).bindAndHandle(httpJsonRoutes.routes, "localhost", 8080)

      serverBinding.onComplete {
        case Success(bound) =>
          ctx.log.info(
            s"HTTP/JSON server online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/"
          )
        case Failure(e) =>
          ctx.log.error(s"HTTP/JSON server could not start!")
      }

      SpawnProtocol()
    }

  implicit val system: ActorSystem[SpawnProtocol.Command] = ActorSystem(apply(), "flixdb")

  private val logger = LoggerFactory.getLogger(classOf[Main])

  // print logo
  logger.info(Logo.Logo)

  // start Kafka migrator
  KafkaMigration(system)

}
