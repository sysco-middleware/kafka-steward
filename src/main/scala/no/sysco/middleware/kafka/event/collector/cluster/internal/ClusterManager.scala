package no.sysco.middleware.kafka.event.collector.cluster.internal

import akka.actor.{ Actor, ActorRef }
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.proto

class ClusterManager(bootstrapServers: String, clusterEventTopic: String) extends Actor {

  val clusterRepository: ActorRef = context.actorOf(ClusterRepository.props(bootstrapServers))
  val clusterEventProducer: ActorRef = context.actorOf(ClusterEventProducer.props(bootstrapServers, clusterEventTopic))
  val nodeManager: ActorRef = context.actorOf(NodeManager.props())

  var cluster: Option[Cluster] = None

  def handleDescribeCluster(): Unit = clusterRepository ! DescribeCluster()

  def handleClusterDescribed(clusterDescribed: ClusterDescribed): Unit = {
    val controller: Option[proto.collector.Node] = clusterDescribed.controller match {
      case Some(c) => Some(Parser.toPb(c))
      case None    => None
    }
    cluster match {
      case None =>
        clusterEventProducer !
          proto.collector.ClusterEvent(
            clusterDescribed.id,
            proto.collector.ClusterEvent.Event.ClusterCreated(proto.collector.ClusterCreated(controller)))
      case Some(current) =>
        val other = Cluster(clusterDescribed.id, clusterDescribed.controller)
        if (!current.equals(other))
          clusterEventProducer !
            proto.collector.ClusterEvent(
              clusterDescribed.id,
              proto.collector.ClusterEvent.Event.ClusterUpdated(proto.collector.ClusterUpdated(controller)))
    }
    nodeManager ! NodesDescribed(clusterDescribed.nodes)
  }

  override def receive: Receive = {
    case DescribeCluster()                  => handleDescribeCluster()
    case clusterDescribed: ClusterDescribed => handleClusterDescribed(clusterDescribed)
  }
}
