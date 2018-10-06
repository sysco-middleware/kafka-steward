package no.sysco.middleware.kafka.event.collector.model

sealed trait Event

case class ClusterDescribed(id: String, controller: Option[Node], nodes: List[Node]) extends Event

case class NodesDescribed(nodes: List[Node]) extends Event

case class TopicsCollected(names: List[String]) extends Event

case class TopicDescribed(topicAndDescription: (String, TopicDescription)) extends Event