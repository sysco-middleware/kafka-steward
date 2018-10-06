package no.sysco.middleware.kafka.event.collector.cluster

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.opencensus.scala.Stats
import io.opencensus.scala.stats.Measurement
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.proto.collector.{NodeCreated, NodeEvent, NodeUpdated}

object NodeManager {
  def props(eventProducer: ActorRef): Props = Props(new NodeManager(eventProducer))

  case class ListNodes()
}

/**
 * Manage Cluster Nodes state.
 * @param eventProducer Reference to producer, to publish events.
 */
class NodeManager(eventProducer: ActorRef) extends Actor with ActorLogging {

  import no.sysco.middleware.kafka.event.collector.metrics.Metrics._
  import NodeManager._

  var nodes: Map[Int, Node] = Map()

  private def evaluateNodesDescribed(listedNodes: List[Node]): Unit = {
    listedNodes match {
      case Nil =>
      case node :: ns =>
        nodes.get(node.id) match {
          case None =>
            Stats.record(List(nodeTypeTag, createdOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
            eventProducer !
              NodeEvent(
                node.id,
                NodeEvent.Event.NodeCreated(
                  NodeCreated(Some(Parser.toPb(node)))))
          case Some(currentNode) =>
            if (!currentNode.equals(node)) {
              Stats.record(List(nodeTypeTag, updatedOperationTypeTag), Measurement.double(totalMessageProducedMeasure, 1))
              eventProducer !
                NodeEvent(
                  node.id,
                  NodeEvent.Event.NodeUpdated(
                    NodeUpdated(Some(Parser.toPb(node)))))
            }
        }
        evaluateNodesDescribed(ns)
    }
  }

  private def evaluateCurrentNodes(currentNodes: List[Node], nodes: List[Node]): Unit = {
    currentNodes match {
      case Nil =>
      case node :: ns =>
        if (!nodes.contains(node))
          log.warning(" is not listed")
        evaluateCurrentNodes(ns, nodes)
    }
  }

  def handleNodesDescribed(nodesDescribed: NodesDescribed): Unit = {
    log.info("Handling {} nodes described event.", nodesDescribed.nodes.size)
    evaluateCurrentNodes(nodes.values.toList, nodesDescribed.nodes)
    evaluateNodesDescribed(nodesDescribed.nodes)
  }

  def handleNodeEvent(nodeEvent: NodeEvent): Unit = {
    log.info("Handling node {} event.", nodeEvent.id)
    nodeEvent.event match {
      case event if event.isNodeCreated =>
        Stats.record(List(nodeTypeTag, createdOperationTypeTag), Measurement.double(totalMessageConsumedMeasure, 1))
        event.nodeCreated match {
          case Some(nodeCreated) =>
            nodes = nodes + (nodeEvent.id -> Parser.fromPb(nodeCreated.getNode))
          case None =>
        }
      case event if event.isNodeUpdated =>
        Stats.record(List(nodeTypeTag, updatedOperationTypeTag), Measurement.double(totalMessageConsumedMeasure, 1))
        event.nodeUpdated match {
          case Some(nodeUpdated) =>
            nodes = nodes + (nodeEvent.id -> Parser.fromPb(nodeUpdated.getNode))
          case None =>
        }
    }
  }

  def handleListNodes(): Unit = sender() ! Nodes(nodes)

  override def receive(): Receive = {
    case nodesDescribed: NodesDescribed => handleNodesDescribed(nodesDescribed)
    case nodeEvent: NodeEvent           => handleNodeEvent(nodeEvent)
    case ListNodes()                    => handleListNodes()
  }
}
