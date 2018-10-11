package no.sysco.middleware.kafka.event.collector.metrics

import io.opencensus.scala.Stats
import io.opencensus.scala.stats._
import no.sysco.middleware.kafka.event.proto.collector.CollectorEvent.EntityType

import scala.util.Try

object Metrics {
  val entityTypeTagName = "entity_type"
  val entityOperationTagName = "entity_operation"
  val tryTotalMessageConsumedMeasure: Try[MeasureDouble] = for {
    measure <- Measure.double("collector_message_consumed", "Total event messages consumed", "by")
    view <- View(
      "collector_message_consumed_total",
      "Total messages consumed by entity type by entity and operation",
      measure,
      List(entityTypeTagName, entityOperationTagName),
      Sum)
    _ <- Stats.registerView(view)
  } yield measure
  val tryTotalMessageProducedMeasure: Try[MeasureDouble] = for {
    measure <- Measure.double("collector_message_produced", "Total event messages produced", "by")
    view <- View(
      "collector_message_produced_total",
      "Total messages produced by entity type by entity and operation",
      measure,
      List(entityTypeTagName, entityOperationTagName),
      Sum)
    _ <- Stats.registerView(view)
  } yield measure
  val totalMessageConsumedMeasure: MeasureDouble = tryTotalMessageConsumedMeasure.get
  val totalMessageProducedMeasure: MeasureDouble = tryTotalMessageProducedMeasure.get
  val clusterTypeTag: Tag = Tag(entityTypeTagName, EntityType.CLUSTER.name).get
  val brokerTypeTag: Tag = Tag(entityTypeTagName, EntityType.BROKER.name).get
  val topicTypeTag: Tag = Tag(entityTypeTagName, EntityType.TOPIC.name).get
  val createdOperationTypeTag: Tag = Tag(entityOperationTagName, "Created").get
  val updatedOperationTypeTag: Tag = Tag(entityOperationTagName, "Updated").get
  val deletedOperationTypeTag: Tag = Tag(entityOperationTagName, "Deleted").get
}
