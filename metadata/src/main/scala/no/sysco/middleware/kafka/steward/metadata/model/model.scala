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

case class Link(source: EntityId, target: EntityId, description: String)

// Generated from different services
case class OpMetadata(entries: Map[String, String] = Map())

// User-managed metadata
case class OrgMetadata(entries: Map[String, String] = Map())

// Helpers
class Topic(topicName: String)
  extends Entity(entityId = EntityId(topicName), entityType = EntityType.Topic)

class Cluster(id: String)
  extends Entity(entityId = EntityId(id), entityType = EntityType.Cluster)

class Broker(id: String)
  extends Entity(entityId = EntityId(id), entityType = EntityType.Broker)

class Rack(id: String, brokers: List[Broker])
  extends Entity(entityId = EntityId(id), entityType = EntityType.Rack)

class Client(id: String)
  extends Entity(entityId = EntityId(id), entityType = EntityType.Client)

class Schema(subjectName: String)
  extends Entity(entityId = EntityId(subjectName), entityType = EntityType.Schema)