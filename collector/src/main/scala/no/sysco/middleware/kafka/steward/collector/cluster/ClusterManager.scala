package no.sysco.middleware.kafka.steward.collector.cluster

import java.time.Duration

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import io.opencensus.scala.Stats
import io.opencensus.scala.stats.Measurement
import no.sysco.middleware.kafka.steward.collector.cluster.BrokerManager.ListBrokers
import no.sysco.middleware.kafka.steward.collector.internal.Parser._
import no.sysco.middleware.kafka.steward.collector.model.{ Cluster, ClusterDescribed, NodesDescribed }
import no.sysco.middleware.kafka.steward.proto.collector
import no.sysco.middleware.kafka.steward.proto.collector.{ BrokerEvent, ClusterCreated, ClusterEvent, ClusterUpdated }

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
  import no.sysco.middleware.kafka.steward.collector.internal.OriginRepository._
  import no.sysco.middleware.kafka.steward.collector.metrics.Metrics._

  val brokerManager: ActorRef =
    context.actorOf(BrokerManager.props(eventRepository, eventProducer), "broker-manager")

  var cluster: Option[Cluster] = None

  override def preStart(): Unit = scheduleDescribeCluster

  override def receive(): Receive = {
    case DescribeCluster() => handleDescribeCluster()
    case clusterDescribed: ClusterDescribed => handleClusterDescribed(clusterDescribed)
    case clusterEvent: ClusterEvent => handleClusterEvent(clusterEvent)
    case GetCluster() => handleGetCluster()
    case brokerEvent: BrokerEvent => brokerManager forward brokerEvent
    case listNodes: ListBrokers => brokerManager forward listNodes
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
    val controller: Option[collector.Node] = clusterDescribed.controller match {
      case Some(c) => Option(toPb(c))
      case None => None
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
            val controller = event.clusterCreated.get.controller match {
              case None => None
              case Some(node) => Some(fromPb(node))
            }
            cluster = Some(Cluster(clusterEvent.id, controller))
      case event if event.isClusterUpdated =>
        Stats.record(
          List(clusterTypeTag, updatedOperationTypeTag),
          Measurement.double(totalMessageConsumedMeasure, 1))
            val controller = event.clusterUpdated.get.controller match {
              case None => None
              case Some(node) => Some(fromPb(node))
            }
            cluster = Some(Cluster(clusterEvent.id, controller))
    }
  }

  def handleGetCluster(): Unit = sender() ! cluster

}
