package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import no.sysco.middleware.kafka.steward.metadata.EntityManager.{OpMetadataUpdated, UpdateOrgMetadata}
import no.sysco.middleware.kafka.steward.metadata.internal.Parser._
import no.sysco.middleware.kafka.steward.metadata.model._
import no.sysco.middleware.kafka.steward.proto.metadata.MetadataEvent

object EntityManager {
  def props(entityEventProducer: ActorRef): Props = Props(new EntityManager(entityEventProducer))

  case class UpdateOrgMetadata(entity: Entity, entry: (String, String))

  case class OpMetadataUpdated(entity: Entity, opMetadata: OpMetadata)

}

class EntityManager(entityEventProducer: ActorRef) extends Actor with ActorLogging {

  var metadata: Map[Entity, (OpMetadata, OrgMetadata)] = Map()

  override def receive: Receive = {
    case entityCreated: EntityCreated => handleEvent(toPb(entityCreated))
    case updateOrgMetadata: UpdateOrgMetadata => handleUpdateOrgMetadata(updateOrgMetadata)
    case opMetadataUpdated: OpMetadataUpdated => handleOpMetadataUpdated(opMetadataUpdated)
  }

  def handleEvent(metadataEvent: MetadataEvent): Unit = {
    val entity = fromPb(metadataEvent.entity.get)
    val opMetadata = fromPb(metadataEvent.opMetadata.get)
    val orgMetadata = fromPb(metadataEvent.orgMetadata.get)
    metadata = metadata + (entity -> (opMetadata, orgMetadata))
  }

  def handleUpdateOrgMetadata(updateOrgMetadata: UpdateOrgMetadata): Unit = {
    val current = metadata(updateOrgMetadata.entity)
    val updated = current._2.entries + updateOrgMetadata.entry
    entityEventProducer ! EntityUpdated(updateOrgMetadata.entity, current._1, OrgMetadata(updated))
  }

  def handleOpMetadataUpdated(opMetadataUpdated: OpMetadataUpdated): Unit = {
    val current = metadata(opMetadataUpdated.entity)
    entityEventProducer ! EntityUpdated(opMetadataUpdated.entity, opMetadataUpdated.opMetadata, current._2)
  }
}
