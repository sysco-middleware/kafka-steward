package no.sysco.middleware.kafka.event.collector.topic.internal

sealed trait State

sealed trait Event

sealed trait Command

case class CollectTopics() extends Command

case class DescribeTopic(name: String) extends Command

case class ListTopics() extends Command

case class TopicsCollected(names: List[String]) extends Event

case class TopicDescribed(topicAndDescription: (String, Description)) extends Event

case class Description(internal: Boolean, partitions: Seq[Partition]) extends State

case class Partition(id: Int, replicas: Seq[Node]) extends State

case class Node(id: Int, host: String, port: Int, rack: String) extends State

case class Topics(topicsAndDescription: Map[String, Option[Description]]) extends State