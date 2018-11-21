package no.sysco.middleware.kafka.steward.collector.topic.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import no.sysco.middleware.kafka.steward.collector.topic.core.model._
import spray.json.{ DefaultJsonProtocol, RootJsonFormat }

trait TopicCollectorJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val brokerIdFormat: RootJsonFormat[BrokerId] = jsonFormat1(BrokerId)
  implicit val replicaFormat: RootJsonFormat[Replica] = jsonFormat1(Replica)
  implicit val configFormat: RootJsonFormat[Config] = jsonFormat1(Config)
  implicit val partitionFormat: RootJsonFormat[Partition] = jsonFormat2(Partition)
  implicit val topicFormat: RootJsonFormat[Topic] = jsonFormat4(Topic)

}
