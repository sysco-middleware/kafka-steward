package no.sysco.middleware.kafka.steward.collector

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector
import no.sysco.middleware.kafka.steward.collector.http.HttpCollectorService

import scala.concurrent.ExecutionContext

/**
 * Application entry point.
 */
object Collector extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("steward-collector")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val collector = actorSystem.actorOf(CollectorManager.props(), "collector")

  val httpCollectorService = new HttpCollectorService(collector)

  val bindingFuture = Http().bindAndHandle(httpCollectorService.route, "0.0.0.0", 8080)

  PrometheusStatsCollector.createAndRegister()
  val server = new io.prometheus.client.exporter.HTTPServer(8081)

  sys.addShutdownHook(
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => actorSystem.terminate()))
}
