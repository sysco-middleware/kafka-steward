package no.sysco.middleware.kafka.event.collector.model

trait State

case class Description(internal: Boolean, partitions: Seq[Partition]) extends State

case class Partition(id: Int, leader: Node, replicas: Seq[Node], isr: Seq[Node]) extends State

case class Node(id: Int, host: String, port: Int, rack: String) extends State

case class Topics(topicsAndDescription: Map[String, Option[Description]]) extends State

case class Nodes(nodes: Map[Int, Node]) extends State