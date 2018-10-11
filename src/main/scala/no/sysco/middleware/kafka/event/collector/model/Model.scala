package no.sysco.middleware.kafka.event.collector.model

sealed trait State

final case class Node(id: Int, host: String, port: Int, rack: Option[String] = Option.empty) extends State

final case class Cluster(id: String, controller: Option[Node]) extends State

final case class Broker(id: String, node: Node, config: Config = Config()) extends State

final case class Brokers(brokers: List[Broker])

final case class Partition(id: Int, leader: Node, replicas: List[Node], isr: List[Node]) extends State

final case class TopicDescription(internal: Boolean, partitions: List[Partition]) extends State

final case class Topic(name: String, description: TopicDescription, config: Config = Config()) extends State

final case class Topics(topics: List[Topic]) extends State

final case class Config(entries: Map[String, String] = Map()) extends State