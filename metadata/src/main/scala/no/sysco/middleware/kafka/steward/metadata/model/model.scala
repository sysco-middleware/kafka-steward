package no.sysco.middleware.kafka.steward.metadata.model

import no.sysco.middleware.kafka.steward.metadata.model.EntityType.EntityType

object EntityType extends Enumeration {
  type EntityType = Value

  val Topic, Cluster, Rack, Broker, Client, Schema = Value
}

case class EntityId(id: String)

case class Entity(entityId: EntityId, entityType: EntityType)

case class EntityState(deleted: Boolean = false, opMetadata: OpMetadata, orgMetadata: OrgMetadata)

// Generated from different services
case class OpMetadata(entries: Map[String, String] = Map())

// User-managed metadata
case class OrgMetadata(entries: Map[String, String] = Map())
