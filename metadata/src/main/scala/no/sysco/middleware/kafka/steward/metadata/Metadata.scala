package no.sysco.middleware.kafka.steward.metadata

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector
import no.sysco.middleware.kafka.steward.metadata.http.HttpMetadataService

import scala.concurrent.ExecutionContext

object Metadata extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("steward-metadata")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val metadata = actorSystem.actorOf(MetadataManager.props(), "metadata-manager")

  val httpMetadataService = new HttpMetadataService(metadata)

  val bindingFuture = Http().bindAndHandle(httpMetadataService.route, "0.0.0.0", 8080)

  PrometheusStatsCollector.createAndRegister()
  val server = new io.prometheus.client.exporter.HTTPServer(8081)

  //  sys.addShutdownHook(
  //    bindingFuture
  //      .flatMap(_.unbind())
  //      .onComplete(_ => actorSystem.terminate()))
}
