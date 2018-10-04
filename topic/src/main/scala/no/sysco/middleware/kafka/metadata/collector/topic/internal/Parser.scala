package no.sysco.middleware.kafka.metadata.collector.topic.internal

import no.sysco.middleware.kafka.metadata.collector.proto.topic.TopicDescriptionPb.TopicPartitionInfoPb
import no.sysco.middleware.kafka.metadata.collector.proto.topic.{ NodePb, TopicDescriptionPb, TopicUpdatedPb }
import org.apache.kafka.clients.admin.TopicDescription

object Parser {
  import scala.collection.JavaConverters._

  def fromKafka(description: TopicDescription): Description =
    Description(
      description.isInternal,
      description.partitions().asScala
        .map(partition =>
          Partition(
            partition.partition(),
            partition.replicas().asScala
              .map(node =>
                Node(node.id(), node.host(), node.port(), node.rack())))))

  def fromPb(name: String, topicDescriptionPb: TopicDescriptionPb): Description =
    Description(
      topicDescriptionPb.internal,
      topicDescriptionPb.topicPartitions
        .map(tp =>
          Partition(
            tp.partition,
            tp.replicas.map(rep => fromPb(rep)))))

  def fromPb(node: NodePb): Node = Node(node.id, node.host, node.port, node.rack)

  def toPb(topicDescription: Description): TopicUpdatedPb =
    TopicUpdatedPb(
      Some(
        TopicDescriptionPb(
          topicDescription.internal,
          topicDescription.partitions
            .map(tpi => TopicPartitionInfoPb(tpi.id, tpi.replicas.map(node => toPb(node)))))))

  def toPb(node: Node): NodePb = NodePb(node.id, node.host, node.port, Option(node.rack).getOrElse(""))
}
