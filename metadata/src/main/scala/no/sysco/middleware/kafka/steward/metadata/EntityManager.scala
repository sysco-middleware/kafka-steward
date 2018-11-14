package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import no.sysco.middleware.kafka.steward.metadata.EntityManager.{DeleteEntity, OpMetadataUpdated, UpdateOrgMetadata}
import no.sysco.middleware.kafka.steward.metadata.internal.Parser._
import no.sysco.middleware.kafka.steward.metadata.model._
import no.sysco.middleware.kafka.steward.proto.metadata.MetadataEvent

object EntityManager {
  def props(entityEventProducer: ActorRef): Props = Props(new EntityManager(entityEventProducer))

  case class UpdateOrgMetadata(entity: Entity, entry: (String, String))

  case class OpMetadataUpdated(entity: Entity, opMetadata: OpMetadata)

  case class DeleteEntity(entity: Entity)

}

class EntityManager(entityEventProducer: ActorRef) extends Actor with ActorLogging {

  var entities: Map[Entity, EntityState] = Map()

  override def receive: Receive = {
    case entityCreated: EntityCreated => handleEvent(toPb(entityCreated))
    case entityUpdated: EntityUpdated => handleEvent(toPb(entityUpdated))
    case entityDeleted: EntityDeleted => handleEvent(toPb(entityDeleted))
    case updateOrgMetadata: UpdateOrgMetadata => handleUpdateOrgMetadata(updateOrgMetadata)
    case opMetadataUpdated: OpMetadataUpdated => handleOpMetadataUpdated(opMetadataUpdated)
    case deleteEntity: DeleteEntity => handleDeleteEntity(deleteEntity)
  }

  def handleEvent(metadataEvent: MetadataEvent): Unit = {
    val entity = fromPb(metadataEvent.entity.get)
    val opMetadata = fromPb(metadataEvent.opMetadata.get)
    val orgMetadata = fromPb(metadataEvent.orgMetadata.get)
    entities = entities + (entity -> EntityState(opMetadata = opMetadata, orgMetadata = orgMetadata))
  }

  def handleUpdateOrgMetadata(updateOrgMetadata: UpdateOrgMetadata): Unit = {
    val current = entities(updateOrgMetadata.entity)
    val updated = current.orgMetadata.entries + updateOrgMetadata.entry
    entityEventProducer ! EntityUpdated(updateOrgMetadata.entity, current.opMetadata, OrgMetadata(updated))
  }

  def handleOpMetadataUpdated(opMetadataUpdated: OpMetadataUpdated): Unit = {
    val current = entities(opMetadataUpdated.entity)
    entityEventProducer ! EntityUpdated(opMetadataUpdated.entity, opMetadataUpdated.opMetadata, current.orgMetadata)
  }

  def handleDeleteEntity(deleteEntity: DeleteEntity): Unit = {
    val current = entities(deleteEntity.entity)
    entityEventProducer ! EntityDeleted(deleteEntity.entity, current.opMetadata, current.orgMetadata)
  }
}
