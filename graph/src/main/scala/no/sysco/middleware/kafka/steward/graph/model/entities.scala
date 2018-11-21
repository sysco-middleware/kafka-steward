package no.sysco.middleware.kafka.steward.graph.model

import no.sysco.middleware.kafka.steward.graph.model.entities.NodeType.NodeType

object entities {

  object NodeType extends Enumeration {
    type NodeType = Value

    val Topic, Partition, Replica, Cluster, Broker, Client, Schema = Value
  }

  type NodeId = Int

  case class Node(id: NodeId, `type`: NodeType, name: String)

  case class Link(source: NodeId, target: NodeId, value: String)

}