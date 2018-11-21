package no.sysco.middleware.kafka.steward.collector.topic

import java.time.Duration

import akka.actor.{ Actor, ActorRef, Props }
import no.sysco.middleware.kafka.steward.collector.topic.core.Topics
import no.sysco.middleware.kafka.steward.collector.topic.core.model.ClusterId
import no.sysco.middleware.kafka.steward.collector.topic.infra.{ OriginSource, TopicRepository, UpstreamSource }
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer

object TopicCollector {
  def props(
    adminClient: AdminClient,
    producer: Producer[Array[Byte], Array[Byte]],
    consumer: Consumer[Array[Byte], Array[Byte]],
    eventTopic: String,
    pollFrequency: Duration): Props = Props(new TopicCollector(adminClient, producer, consumer, eventTopic, pollFrequency))
}

class TopicCollector(
  adminClient: AdminClient,
  producer: Producer[Array[Byte], Array[Byte]],
  consumer: Consumer[Array[Byte], Array[Byte]],
  eventTopic: String,
  pollFrequency: Duration) extends Actor {

  val clusterId: ClusterId = ClusterId(adminClient.describeCluster().clusterId().get())

  val topicRepository: ActorRef = context.actorOf(TopicRepository.props(producer, eventTopic))
  val topics: ActorRef = context.actorOf(Topics.props(clusterId, topicRepository))
  val upstreamSource: ActorRef = context.actorOf(UpstreamSource.props(adminClient, topics, pollFrequency))
  val originSource: ActorRef = context.actorOf(OriginSource.props(consumer, eventTopic, topics))

  override def receive: Receive = Actor.emptyBehavior
}
