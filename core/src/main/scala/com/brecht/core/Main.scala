package com.brecht.core

import akka.actor
import akka.actor.typed._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.brecht.cdc.scaladsl.ChangeDataCapture
import com.brecht.core.postgresql.PostgreSQLActor
import com.lonelyplanet.prometheus.api.MetricsEndpoint
import io.prometheus.client.CollectorRegistry
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

      val changeDataCapture = {
        import com.brecht.cdc.{PostgreSQLActor, _}
        val dataSource = HikariCP(system).startHikariDataSource("postgresql-cdc-pool", metrics = false)
        val postgreSQLActor = ctx.spawn(
          behavior = PostgreSQLActor.apply(PostgreSQL(dataSource)),
          name = PostgreSQLActor.name,
          props = PostgreSQLActor.props
        )
        ChangeDataCapture(PostgreSQLInstance(postgreSQLActor))
      }

      val postgreSQLActor: ActorRef[PostgreSQLActor.Request] = PostgreSQLActor.getRouter(ctx)
      val eventStoreController = new EventStoreControllerImpl(postgreSQLActor)

      val httpJsonRoutes = new HttpJsonRoutes(eventStoreController)

      val config = BrechtConfig(system)

      val serverBinding: Future[Http.ServerBinding] =
        Http()(untypedSystem).bindAndHandle(httpJsonRoutes.routes, config.host, config.port)

      serverBinding.onComplete {
        case Success(bound) =>
          ctx.log.info(
            s"HTTP/JSON server online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/"
          )
        case Failure(e) =>
          ctx.log.error(s"HTTP/JSON server could not start!")
      }

      val metricsEndpoint = new MetricsEndpoint(CollectorRegistry.defaultRegistry)
      val routes = metricsEndpoint.routes
      val bindingFuture = Http().bindAndHandle(routes, interface = config.promHost, port = config.promPort)
      bindingFuture.onComplete {
        case Success(bound) =>
          ctx.log.info(
            s"HTTP server with Prometheus /metrics endpoint online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/"
          )
        case Failure(e) =>
          ctx.log.error(s"HTTP server with Prometheus /metrics endpoint could not start!")
      }

      KafkaMigration(changeDataCapture)

      SpawnProtocol()
    }

  implicit val system: ActorSystem[SpawnProtocol.Command] = ActorSystem(apply(), "brecht")

  private val logger = LoggerFactory.getLogger(classOf[Main])

  // print logo
  logger.info(Logo.Logo)

}
