package no.sysco.middleware.kafka.event.collector.cluster

import akka.actor.{ Actor, ActorRef, Props }
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.proto.collector.{ NodeCreated, NodeEvent, NodeUpdated }

object NodeManager {
  def props(eventProducer: ActorRef): Props = Props(new NodeManager(eventProducer))

  case class ListNodes()
}

/**
 * Manage Cluster Nodes state.
 * @param eventProducer Reference to producer, to publish events.
 */
class NodeManager(eventProducer: ActorRef) extends Actor {

  import NodeManager._

  var nodes: Map[Int, Node] = Map()

  def evaluateNodesDescribed(listedNodes: List[Node]): Unit = {
    listedNodes match {
      case Nil =>
      case node :: ns =>
        nodes.get(node.id) match {
          case None =>
            eventProducer !
              NodeEvent(
                node.id,
                NodeEvent.Event.NodeCreated(
                  NodeCreated(Some(Parser.toPb(node)))))
          case Some(currentNode) =>
            if (!currentNode.equals(node)) {
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

  def evaluateCurrentNodes(currentNodes: List[Node], nodes: List[Node]): Unit = {
    currentNodes match {
      case Nil =>
      case node :: ns =>
        if (!nodes.contains(node))
          //TODO eval if nodes could appear as removed
          evaluateCurrentNodes(ns, nodes)
    }
  }

  def handleNodesDescribed(nodesDescribed: NodesDescribed): Unit = {
    evaluateCurrentNodes(nodes.values.toList, nodesDescribed.nodes)
    evaluateNodesDescribed(nodesDescribed.nodes)
  }

  def handleNodeEvent(nodeEvent: NodeEvent): Unit = {
    nodeEvent.event match {
      case event if event.isNodeCreated =>
        event.nodeCreated match {
          case Some(nodeCreated) =>
            nodes = nodes + (nodeEvent.id -> Parser.fromPb(nodeCreated.getNode))
          case None =>
        }
      case event if event.isNodeUpdated =>
        event.nodeUpdated match {
          case Some(nodeUpdated) =>
            nodes = nodes + (nodeEvent.id -> Parser.fromPb(nodeUpdated.getNode))
          case None =>
        }
    }
  }

  def handleListNodes(): Unit = sender() ! Nodes(nodes)

  override def receive: Receive = {
    case nodesDescribed: NodesDescribed => handleNodesDescribed(nodesDescribed)
    case nodeEvent: NodeEvent           => handleNodeEvent(nodeEvent)
    case ListNodes()                    => handleListNodes()
  }
}
