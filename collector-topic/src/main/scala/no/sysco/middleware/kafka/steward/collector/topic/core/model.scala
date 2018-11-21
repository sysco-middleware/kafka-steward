package no.sysco.middleware.kafka.steward.collector.topic.core

object model {
  // State classes
  case class Topic(name: String, partitions: Set[Partition], config: Config = Config(Map()), internal: Boolean = false)

  case class Partition(id: Int, replicas: Set[Replica])

  case class Replica(brokerId: BrokerId)

  case class BrokerId(id: Int)

  case class ClusterId(id: String)

  case class Config(entries: Map[String, String])
  // Topic classes
  sealed trait TopicEvent

  case class TopicsCollected(topics: List[String]) extends TopicEvent

  case class TopicDescribed(name: String, topic: Topic) extends TopicEvent

  case class TopicCreated(clusterId: ClusterId, name: String) extends TopicEvent

  case class TopicUpdated(clusterId: ClusterId, topic: Topic) extends TopicEvent

  case class TopicDeleted(clusterId: ClusterId, name: String) extends TopicEvent
}
