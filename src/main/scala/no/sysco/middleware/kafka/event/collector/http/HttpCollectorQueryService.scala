package no.sysco.middleware.kafka.event.collector.http

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.{ Directives, Route }
import akka.pattern.ask
import akka.util.Timeout
import no.sysco.middleware.kafka.event.collector.cluster.ClusterManager.GetCluster
import no.sysco.middleware.kafka.event.collector.cluster.BrokerManager.ListNodes
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.collector.topic.TopicManager.ListTopics
import spray.json._

import scala.concurrent.duration._

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val nodeFormat: RootJsonFormat[Node] = jsonFormat4(Node)
  implicit val configFormat: RootJsonFormat[Config] = jsonFormat1(Config)
  implicit val brokerFormat: RootJsonFormat[Broker] = jsonFormat3(Broker)
  implicit val partitionFormat: RootJsonFormat[Partition] = jsonFormat4(Partition)
  implicit val topicDescriptionFormat: RootJsonFormat[TopicDescription] = jsonFormat2(TopicDescription)
  implicit val topicFormat: RootJsonFormat[Topic] = jsonFormat3(Topic)
  implicit val topicsFormat: RootJsonFormat[Topics] = jsonFormat1(Topics)
  implicit val clusterFormat: RootJsonFormat[Cluster] = jsonFormat2(Cluster)
  implicit val nodesFormat: RootJsonFormat[Brokers] = jsonFormat1(Brokers)
}

class HttpCollectorQueryService(collector: ActorRef) extends Directives with JsonSupport {
  implicit val timeout: Timeout = 5.seconds

  val route: Route =
    path("topics") {
      get {
        complete((collector ? ListTopics()).mapTo[Topics])
      }
    } ~
      path("cluster") {
        get {
          complete((collector ? GetCluster()).mapTo[Option[Cluster]])
        }
      } ~
      path("brokers") {
        get {
          complete((collector ? ListNodes()).mapTo[Brokers])
        }
      }

}
