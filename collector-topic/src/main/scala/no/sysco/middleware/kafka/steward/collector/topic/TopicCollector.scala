package no.sysco.middleware.kafka.steward.collector.topic

import java.time.Duration

import akka.actor.{ Actor, ActorRef, Props }
import no.sysco.middleware.kafka.steward.collector.topic.TopicCollector.GetTopics
import no.sysco.middleware.kafka.steward.collector.topic.core.Topics
import no.sysco.middleware.kafka.steward.collector.topic.core.Topics.GetState
import no.sysco.middleware.kafka.steward.collector.topic.core.model.ClusterId
import no.sysco.middleware.kafka.steward.collector.topic.infra.UpstreamSource.ListTopics
import no.sysco.middleware.kafka.steward.collector.topic.infra.{ OriginRepository, OriginSource, UpstreamSource }
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer

object TopicCollector {
  def props(
    adminClient: AdminClient,
    producer: Producer[Array[Byte], Array[Byte]],
    consumer: Consumer[Array[Byte], Array[Byte]],
    eventTopic: String,
    pollFrequency: Duration): Props =
    Props(
      new TopicCollector(adminClient, producer, consumer, eventTopic, pollFrequency))

  case class GetTopics()
}

class TopicCollector(
  adminClient: AdminClient,
  producer: Producer[Array[Byte], Array[Byte]],
  consumer: Consumer[Array[Byte], Array[Byte]],
  eventTopic: String,
  pollFrequency: Duration) extends Actor {

  private val scheduler = context.system.scheduler

  val clusterId: ClusterId = ClusterId(adminClient.describeCluster().clusterId().get())

  val topicRepository: ActorRef = context.actorOf(OriginRepository.props(producer, eventTopic), "origin-repository")
  val topics: ActorRef = context.actorOf(Topics.props(clusterId, topicRepository), "topics")
  val originSource: ActorRef = context.actorOf(OriginSource.props(consumer, eventTopic, topics), "origin-source")
  val upstreamSource: ActorRef = context.actorOf(UpstreamSource.props(adminClient, topics, pollFrequency), "upstream-source")

  override def preStart(): Unit = {
    scheduler.scheduleOnce(pollFrequency, upstreamSource, ListTopics(), context.dispatcher, self)
  }

  override def receive: Receive = {
    case GetTopics() => topics.forward(GetState())
  }
}
