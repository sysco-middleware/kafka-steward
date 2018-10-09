package no.sysco.middleware.kafka.event.collector.model

sealed trait State

final case class Node(id: Int, host: String, port: Int, rack: Option[String] = Option.empty) extends State

final case class Cluster(id: String, controller: Option[Node]) extends State

final case class Nodes(nodes: Map[String, Node]) extends State

final case class Partition(id: Int, leader: Node, replicas: List[Node], isr: List[Node]) extends State

final case class TopicDescription(internal: Boolean, partitions: List[Partition]) extends State

final case class Topics(topicsAndDescription: Map[String, Option[TopicDescription]]) extends State
