package no.sysco.middleware.kafka.event.collector.topic.internal

import java.util.Properties

import akka.actor.{ Actor, ActorRef, Props }
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }

import scala.collection.JavaConverters._

object TopicRepository {
  def props(adminClient: AdminClient): Props = Props(new TopicRepository(adminClient))
  def props(bootstrapServers: String): Props = {
    val adminConfigs = new Properties()
    adminConfigs.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    Props(new TopicRepository(AdminClient.create(adminConfigs)))
  }
}

/**
 * Query Topics and details from a Kafka cluster.
 *
 * @param adminClient Client to connect to a Kafka Cluster.
 */
class TopicRepository(adminClient: AdminClient) extends Actor {

  def handleCollectTopics(): Unit = {
    val thisSender: ActorRef = sender()
    adminClient.listTopics()
      .names()
      .thenApply(names => thisSender ! TopicsCollected(names.asScala.toList))
  }

  def handleDescribeTopic(describeTopic: DescribeTopic): Unit = {
    val thisSender: ActorRef = sender()
    adminClient.describeTopics(List(describeTopic.name).asJava)
      .all()
      .thenApply(topicsAndDescription =>
        thisSender !
          TopicDescribed(
            describeTopic.name,
            Parser.fromKafka(topicsAndDescription.get(describeTopic.name))))
  }

  override def receive: Receive = {
    case CollectTopics()               => handleCollectTopics()
    case describeTopics: DescribeTopic => handleDescribeTopic(describeTopics)
  }
}
