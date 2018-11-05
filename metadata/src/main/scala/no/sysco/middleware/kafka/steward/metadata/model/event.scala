package no.sysco.middleware.kafka.steward.metadata.model

sealed trait Event

case class EntityCreated(entity: Entity, opMetadata: OpMetadata) extends Event

case class EntityUpdated(entity: Entity, opMetadata: OpMetadata, orgMetadata: OrgMetadata) extends Event

case class EntityDeleted(entity: Entity) extends Event
