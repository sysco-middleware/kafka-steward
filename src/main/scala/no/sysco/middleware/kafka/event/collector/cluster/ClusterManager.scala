package no.sysco.middleware.kafka.event.collector.cluster

import java.time.Duration

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import no.sysco.middleware.kafka.event.collector.cluster.NodeManager.ListNodes
import no.sysco.middleware.kafka.event.collector.model.{ Cluster, ClusterDescribed, NodesDescribed, Parser }
import no.sysco.middleware.kafka.event.proto.collector._

import scala.concurrent.ExecutionContext

object ClusterManager {
  def props(pollInterval: Duration, eventRepository: ActorRef, eventProducer: ActorRef)(implicit executionContext: ExecutionContext) =
    Props(new ClusterManager(pollInterval, eventProducer, eventProducer))

  case class GetCluster()
}

/**
 * Manage Cluster state.
 *
 * @param pollInterval    How often to query Cluster state.
 * @param eventRepository Reference to repository where to query.
 * @param eventProducer   Reference to producer, to publish events.
 */
class ClusterManager(pollInterval: Duration, eventRepository: ActorRef, eventProducer: ActorRef)(implicit executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  import ClusterManager._
  import no.sysco.middleware.kafka.event.collector.internal.EventRepository._

  val nodeManager: ActorRef = context.actorOf(NodeManager.props(eventRepository), "node-manager")

  var cluster: Option[Cluster] = None

  def handleDescribeCluster(): Unit = {
    log.info("Handling describe cluster command.")
    eventRepository ! DescribeCluster()

    scheduleDescribeCluster
  }

  private def scheduleDescribeCluster = {
    context.system.scheduler.scheduleOnce(pollInterval, () => self ! DescribeCluster())
  }

  def handleClusterDescribed(clusterDescribed: ClusterDescribed): Unit = {
    log.info(s"Handling cluster ${clusterDescribed.id} described event.")
    val controller: Option[Node] = clusterDescribed.controller match {
      case Some(c) => Some(Parser.toPb(c))
      case None    => None
    }
    cluster match {
      case None =>
        eventProducer !
          ClusterEvent(
            clusterDescribed.id,
            ClusterEvent.Event.ClusterCreated(ClusterCreated(controller)))
      case Some(current) =>
        val other = Cluster(clusterDescribed.id, clusterDescribed.controller)
        if (!current.equals(other))
          eventProducer !
            ClusterEvent(
              clusterDescribed.id,
              ClusterEvent.Event.ClusterUpdated(ClusterUpdated(controller)))
    }
    nodeManager ! NodesDescribed(clusterDescribed.nodes)
  }

  def handleClusterEvent(clusterEvent: ClusterEvent): Unit = {
    log.info(s"Handling cluster ${clusterEvent.id} event.")
    clusterEvent.event match {
      case event if event.isClusterCreated =>
        event.clusterCreated match {
          case Some(clusterCreated) =>
            val controller = clusterCreated.controller match {
              case None       => None
              case Some(node) => Some(Parser.fromPb(node))
            }
            cluster = Some(Cluster(clusterEvent.id, controller))
          case None =>
        }
      case event if event.isClusterUpdated =>
        event.clusterUpdated match {
          case Some(clusterUpdated) =>
            val controller = clusterUpdated.controller match {
              case None       => None
              case Some(node) => Some(Parser.fromPb(node))
            }
            cluster = Some(Cluster(clusterEvent.id, controller))
          case None =>
        }
    }
  }

  def handleGetCluster(): Unit = sender() ! cluster

  def handleNodeEvent(nodeEvent: NodeEvent): Unit = nodeManager forward nodeEvent

  override def preStart(): Unit = scheduleDescribeCluster

  override def receive(): Receive = {
    case DescribeCluster()                  => handleDescribeCluster()
    case clusterDescribed: ClusterDescribed => handleClusterDescribed(clusterDescribed)
    case clusterEvent: ClusterEvent         => handleClusterEvent(clusterEvent)
    case nodeEvent: NodeEvent               => handleNodeEvent(nodeEvent)
    case GetCluster()                       => handleGetCluster()
    case listNodes: ListNodes               => nodeManager forward listNodes
  }
}
