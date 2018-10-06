package no.sysco.middleware.kafka.event.collector

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import no.sysco.middleware.kafka.event.collector.http.HttpCollectorQueryService

import scala.concurrent.ExecutionContext

/**
 * Application entry point.
 */
object Collector extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("collector-system")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val collector = actorSystem.actorOf(CollectorManager.props(), "collector")

  val httpCollectorQueryService = new HttpCollectorQueryService(collector)

  val bindingFuture = Http().bindAndHandle(httpCollectorQueryService.route, "0.0.0.0", 8080)

  sys.addShutdownHook(
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => actorSystem.terminate()))
}
