package no.sysco.middleware.kafka.steward.collector.topic

import akka.actor.{Actor, ActorRef, Props}
import no.sysco.middleware.kafka.steward.collector.topic.core.Topics
import no.sysco.middleware.kafka.steward.collector.topic.core.model.ClusterId
import no.sysco.middleware.kafka.steward.collector.topic.infra.{OriginSource, TopicRepository, UpstreamSource}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer

object TopicCollector {
  def props(adminClient: AdminClient,
            producer: Producer[Array[Byte], Array[Byte]],
            consumer: Consumer[Array[Byte], Array[Byte]]): Props = Props(new TopicCollector(adminClient, producer, consumer))
}

class TopicCollector(
                      adminClient: AdminClient,
                      producer: Producer[Array[Byte], Array[Byte]],
                      consumer: Consumer[Array[Byte], Array[Byte]]) extends Actor {

  val clusterId: ClusterId = ClusterId(adminClient.describeCluster().clusterId().get())

  val topicRepository: ActorRef = context.actorOf(TopicRepository.props(producer, "topic"))
  val topics: ActorRef = context.actorOf(Topics.props(clusterId, topicRepository))
  val upstreamSource: ActorRef = context.actorOf(UpstreamSource.props(adminClient, topics))
  val originSource: ActorRef = context.actorOf(OriginSource.props(consumer, "topic", topics))

  override def receive: Receive = Actor.emptyBehavior
}
