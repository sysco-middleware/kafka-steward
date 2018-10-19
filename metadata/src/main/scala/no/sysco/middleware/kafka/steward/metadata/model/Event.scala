package no.sysco.middleware.kafka.steward.metadata.model

sealed trait Event

case class EntityCreated() extends Event

case class EntityUpdated() extends Event

case class EntityDeleted() extends Event

case class EntityMetadataAdded() extends Event

case class EntityMetadataUpdated() extends Event

case class EntityMetadataRemoved() extends Event