package no.sysco.middleware.kafka.event.collector.model

sealed trait Event

sealed trait Command

sealed trait State

case class DescribeCluster() extends Command

case class Cluster(id: String, controller: Option[Node]) extends State

case class ClusterDescribed(id: String, controller: Option[Node], nodes: List[Node]) extends Event

case class NodesDescribed(nodes: List[Node]) extends Event

case class CollectTopics() extends Command

case class DescribeTopic(name: String) extends Command

case class ListTopics() extends Command

case class TopicsCollected(names: List[String]) extends Event

case class TopicDescribed(topicAndDescription: (String, Description)) extends Event

case class Description(internal: Boolean, partitions: Seq[Partition]) extends State

case class Partition(id: Int, leader: Node, replicas: Seq[Node], isr: Seq[Node]) extends State

case class Node(id: Int, host: String, port: Int, rack: String) extends State

case class Topics(topicsAndDescription: Map[String, Option[Description]]) extends State