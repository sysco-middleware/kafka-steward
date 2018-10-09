package no.sysco.middleware.kafka.event.collector.cluster

import java.time.Duration

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import io.opencensus.scala.Stats
import io.opencensus.scala.stats.Measurement
import no.sysco.middleware.kafka.event.collector.cluster.BrokerManager.ListNodes
import no.sysco.middleware.kafka.event.collector.internal.Parser
import no.sysco.middleware.kafka.event.collector.model.{ Cluster, ClusterDescribed, NodesDescribed }
import no.sysco.middleware.kafka.event.proto.collector._

import scala.concurrent.ExecutionContext

object ClusterManager {
  def props(
    pollInterval: Duration,
    eventRepository: ActorRef,
    eventProducer: ActorRef)(implicit executionContext: ExecutionContext) =
    Props(new ClusterManager(pollInterval, eventRepository, eventProducer))

  case class GetCluster()

}

/**
 * Manage Cluster state.
 *
 * @param pollInterval    How often to query Cluster state.
 * @param eventRepository Reference to repository where to query.
 * @param eventProducer   Reference to producer, to publish events.
 */
class ClusterManager(
    pollInterval: Duration,
    eventRepository: ActorRef,
    eventProducer: ActorRef)(implicit executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  import ClusterManager._
  import no.sysco.middleware.kafka.event.collector.internal.EventRepository._
  import no.sysco.middleware.kafka.event.collector.metrics.Metrics._

  val brokerManager: ActorRef = context.actorOf(BrokerManager.props(eventProducer), "broker-manager")

  var cluster: Option[Cluster] = None

  override def preStart(): Unit = scheduleDescribeCluster

  override def receive(): Receive = {
    case DescribeCluster()                  => handleDescribeCluster()
    case clusterDescribed: ClusterDescribed => handleClusterDescribed(clusterDescribed)
    case clusterEvent: ClusterEvent         => handleClusterEvent(clusterEvent)
    case GetCluster()                       => handleGetCluster()
    case nodeEvent: NodeEvent               => brokerManager forward nodeEvent
    case listNodes: ListNodes               => brokerManager forward listNodes
  }

  def handleDescribeCluster(): Unit = {
    log.info("Handling describe cluster command.")
    eventRepository ! DescribeCluster()

    scheduleDescribeCluster
  }

  private def scheduleDescribeCluster = {
    context.system.scheduler.scheduleOnce(pollInterval, () => self ! DescribeCluster())
  }

  def handleClusterDescribed(clusterDescribed: ClusterDescribed): Unit = {
    log.info("Handling cluster {} described event.", clusterDescribed.id)
    val controller: Option[Node] = clusterDescribed.controller match {
      case Some(c) => Some(Parser.toPb(c))
      case None    => None
    }
    cluster match {
      case None =>
        Stats.record(
          List(clusterTypeTag, createdOperationTypeTag),
          Measurement.double(totalMessageProducedMeasure, 1))
        cluster = Some(Cluster(clusterDescribed.id, clusterDescribed.controller))
        eventProducer ! ClusterEvent(clusterDescribed.id, ClusterEvent.Event.ClusterCreated(ClusterCreated(controller)))
      case Some(thisCluster) =>
        val thatCluster = Cluster(clusterDescribed.id, clusterDescribed.controller)
        if (!thisCluster.equals(thatCluster)) {
          Stats.record(
            List(clusterTypeTag, updatedOperationTypeTag),
            Measurement.double(totalMessageProducedMeasure, 1))
          cluster = Some(Cluster(clusterDescribed.id, clusterDescribed.controller))
          eventProducer ! ClusterEvent(clusterDescribed.id, ClusterEvent.Event.ClusterUpdated(ClusterUpdated(controller)))
        }
    }
    brokerManager ! NodesDescribed(clusterDescribed.nodes)
  }

  def handleClusterEvent(clusterEvent: ClusterEvent): Unit = {
    log.info("Handling cluster {} event.", clusterEvent.id)
    clusterEvent.event match {
      case event if event.isClusterCreated =>
        Stats.record(
          List(clusterTypeTag, createdOperationTypeTag),
          Measurement.double(totalMessageConsumedMeasure, 1))
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
        Stats.record(
          List(clusterTypeTag, updatedOperationTypeTag),
          Measurement.double(totalMessageConsumedMeasure, 1))
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

}
