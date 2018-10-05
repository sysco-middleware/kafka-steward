package no.sysco.middleware.kafka.event.collector.internal

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorRef, Props }
import no.sysco.middleware.kafka.event.collector.model.{ ClusterDescribed, Parser, TopicDescribed, TopicsCollected }
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }

import scala.collection.JavaConverters._

object EventRepository {
  def props(adminClient: AdminClient): Props = Props(new EventRepository(adminClient))
  def props(bootstrapServers: String): Props = {
    val adminConfigs = new Properties()
    adminConfigs.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    Props(new EventRepository(AdminClient.create(adminConfigs)))
  }

  case class DescribeCluster()
  case class CollectTopics()
  case class DescribeTopic(name: String)
}

/**
 * Query Cluster details from a Kafka cluster.
 *
 * @param adminClient Client to connect to a Kafka Cluster.
 */
class EventRepository(adminClient: AdminClient) extends Actor {

  import EventRepository._

  def handleDescribeCluster(): Unit = {
    val thisSender: ActorRef = sender()
    val clusterResult = adminClient.describeCluster()
    clusterResult.clusterId()
      .thenApply { clusterId =>
        clusterResult.controller()
          .thenApply { node =>
            clusterResult.nodes()
              .thenApply { nodes =>
                thisSender !
                  ClusterDescribed(
                    clusterId,
                    Some(Parser.fromKafka(node)),
                    nodes.asScala.toList.map(node => Parser.fromKafka(node)))
              }
          }
      }
  }

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
    case DescribeCluster()             => handleDescribeCluster()
    case CollectTopics()               => handleCollectTopics()
    case describeTopics: DescribeTopic => handleDescribeTopic(describeTopics)
  }

  override def postStop(): Unit = adminClient.close(1, TimeUnit.SECONDS)
}
