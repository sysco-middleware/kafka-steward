package no.sysco.middleware.kafka.steward.metadata.internal

import no.sysco.middleware.kafka.steward.metadata.model._
import no.sysco.middleware.kafka.steward.proto
import no.sysco.middleware.kafka.steward.proto.metadata.MetadataEvent

object Parser {
  def toPb(entityCreated: EntityCreated): MetadataEvent =
    MetadataEvent(toPb(entityCreated.entity), toPb(entityCreated.opMetadata))

  def toPb(entityUpdated: EntityUpdated): MetadataEvent =
    MetadataEvent(toPb(entityUpdated.entity), toPb(entityUpdated.opMetadata), toPb(entityUpdated.orgMetadata))

  def toPb(entityDeleted: EntityDeleted): MetadataEvent =
    MetadataEvent(toPb(entityDeleted.entity), deleted = true)

  def toPb(entity: Entity): Option[proto.entity.Entity] =
    Some(proto.entity.Entity(
      entity.entityType match {
        case EntityType.Broker => proto.entity.Entity.EntityType.BROKER
        case EntityType.Cluster => proto.entity.Entity.EntityType.CLUSTER
        case EntityType.Topic => proto.entity.Entity.EntityType.TOPIC
      },
      entity.entityId.id))

  def toPb(opMetadata: OpMetadata): Option[proto.metadata.OpMetadata] =
    Some(proto.metadata.OpMetadata(opMetadata.entries))

  def toPb(orgMetadata: OrgMetadata): Option[proto.metadata.OrgMetadata] =
    Some(proto.metadata.OrgMetadata(orgMetadata.entries))

  def fromPb(entity: proto.entity.Entity): Entity =
    Entity(
      EntityId(entity.id),
      entity.`type` match {
        case proto.entity.Entity.EntityType.BROKER => EntityType.Broker
        case proto.entity.Entity.EntityType.CLUSTER => EntityType.Cluster
        case proto.entity.Entity.EntityType.TOPIC => EntityType.Topic
      })

  def fromPb(opMetadata: proto.metadata.OpMetadata): OpMetadata = OpMetadata(opMetadata.entries)

  def fromPb(orgMetadata: proto.metadata.OrgMetadata): OrgMetadata = OrgMetadata(orgMetadata.entries)
}
