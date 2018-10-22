package no.sysco.middleware.kafka.steward.metadata.model

import no.sysco.middleware.kafka.steward.metadata.model.EntityType.EntityType

object EntityType extends Enumeration {
  type EntityType = Value

  val Topic, TopicPartition, TopicPartitionReplica, Cluster, Rack, Broker, Client, Schema = Value
}

object EntityStatus extends Enumeration {
  type EntityStatus = Value

  val Current, Removed = Value
}

case class EntityId(id: String)

case class Entity(entityId: EntityId, entityType: EntityType)

case class Metadata(opMetadata: OpMetadata = OpMetadata(), orgMetadata: OrgMetadata = OrgMetadata())

case class OpMetadata(entries: Map[String, String] = Map())

case class OrgMetadata(entries: Map[String, String] = Map())

case class Relation(name: String, source: Entity, destination: Entity)

case class Topic(topicName: String)
  extends Entity(entityId = EntityId(topicName), entityType = EntityType.Topic)

case class TopicPartition(topicName: String, id: Int)
  extends Entity(entityId = EntityId(s"$topicName-$id"), entityType = EntityType.TopicPartition)

case class TopicPartitionReplica(id: Int, leader: Boolean)
  extends Entity(entityId = EntityId(s""), entityType = EntityType.TopicPartitionReplica)

case class Cluster(id: String)
  extends Entity(entityId = EntityId(id), entityType = EntityType.Cluster)

case class Broker(id: String)
  extends Entity(entityId = EntityId(id), entityType = EntityType.Broker)

case class Rack(id: String, brokers: List[Broker])
  extends Entity(entityId = EntityId(id), entityType = EntityType.Rack)

case class Client(id: String)
  extends Entity(entityId = EntityId(id), entityType = EntityType.Client)

case class Schema(subjectName: String)
  extends Entity(entityId = EntityId(subjectName), entityType = EntityType.Schema)