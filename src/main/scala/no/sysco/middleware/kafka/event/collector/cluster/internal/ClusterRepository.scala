package no.sysco.middleware.kafka.event.collector.cluster.internal

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorRef, Props }
import no.sysco.middleware.kafka.event.collector.model._
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }

import scala.collection.JavaConverters._

object ClusterRepository {
  def props(bootstrapServers: String): Props = {
    val adminConfigs = new Properties()
    adminConfigs.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    Props(new ClusterRepository(AdminClient.create(adminConfigs)))

  }

  def props(adminClient: AdminClient): Props = Props(new ClusterRepository(adminClient))
}

/**
 * Query Cluster details from a Kafka cluster.
 *
 * @param adminClient Client to connect to a Kafka Cluster.
 */
class ClusterRepository(adminClient: AdminClient) extends Actor {

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

  override def receive: Receive = {
    case DescribeCluster() => handleDescribeCluster()
  }

  override def postStop(): Unit = adminClient.close(1, TimeUnit.SECONDS)
}
