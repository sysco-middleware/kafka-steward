package no.sysco.middleware.kafka.steward.metadata.model

case class Entity(entityId: String, entityType: String)

case class Metadata(opMetadata: OpMetadata, orgMetadata: OrgMetadata)

case class OpMetadata(entries: Map[String, String])

case class OrgMetadata(entries: Map[String, String])

case class Relation(name: String, source: String, destination: String)

case class Topic(topicName: String, metadata: Metadata, partitions: List[Relation])
  extends Entity(entityId = topicName, entityType = "Topic")

case class Partition(topicName: String,
                     id: Int,
                     metadata: Metadata,
                     replicas: List[Relation],
                     isr: List[Relation])
  extends Entity(entityId = s"$topicName-$id", entityType = "TopicPartition")

case class Replica(id: String, leader: Boolean, broker: Relation)
  extends Entity(entityId = s"", entityType = "TopicPartitionReplica")

case class Cluster(id: String, brokers: List[Relation])
  extends Entity(entityId = id, entityType = "Cluster")

case class Broker(id: String, metadata: Metadata)
  extends Entity(entityId = id, entityType = "Broker")

case class Rack(id: String, brokers: List[Relation])
  extends Entity(entityId = id, entityType = "Rack")

case class Client(id: String, metadata: Metadata, producing: List[Relation], consuming: List[Relation])
  extends Entity(entityId = id, entityType = "Client")

case class Schema(subjectName: String, metadata: Metadata, usedBy: List[Relation])
extends Entity(entityId = subjectName, entityType = "Schema")