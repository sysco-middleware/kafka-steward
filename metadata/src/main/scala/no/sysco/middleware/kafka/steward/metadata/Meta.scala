package no.sysco.middleware.kafka.steward.metadata

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector

import scala.concurrent.ExecutionContext

object Meta extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("collector-system")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

//  val collector = actorSystem.actorOf(CollectorManager.props(), "collector")

//  val httpCollectorQueryService = new HttpCollectorQueryService(collector)

//  val bindingFuture = Http().bindAndHandle(httpCollectorQueryService.route, "0.0.0.0", 8080)

  PrometheusStatsCollector.createAndRegister()
  val server = new io.prometheus.client.exporter.HTTPServer(8081)

//  sys.addShutdownHook(
//    bindingFuture
//      .flatMap(_.unbind())
//      .onComplete(_ => actorSystem.terminate()))
}
