package no.sysco.middleware.kafka.event.collector.internal

import no.sysco.middleware.kafka.event.collector.model.{ Node, Partition, TopicDescription }
import no.sysco.middleware.kafka.event.proto
import org.apache.kafka.clients.admin
import org.apache.kafka.common

/**
 * Translate between sources as Kafka API and Protocol Buffers
 */
object Parser {
  import scala.collection.JavaConverters._

  def fromKafka(topicDescription: admin.TopicDescription): TopicDescription =
    TopicDescription(
      topicDescription.isInternal,
      topicDescription.partitions().asScala
        .map(partition =>
          Partition(
            partition.partition(),
            fromKafka(partition.leader()),
            partition.replicas().asScala
              .map(node =>
                fromKafka(node)),
            partition.isr().asScala
              .map(node =>
                fromKafka(node)))))

  def fromKafka(node: common.Node): Node = {
    Node(node.id(), node.host(), node.port(), node.rack())
  }

  def fromPb(name: String, topicDescription: proto.collector.TopicDescription): TopicDescription =
    TopicDescription(
      topicDescription.internal,
      topicDescription.topicPartitions
        .map(tp =>
          Partition(
            tp.partition,
            fromPb(tp.leader.get),
            tp.replicas.map(rep => fromPb(rep)),
            tp.isr.map(rep => fromPb(rep)))))

  def fromPb(node: proto.collector.Node): Node = Node(node.id, node.host, node.port, node.rack)

  def toPb(topicDescription: TopicDescription): proto.collector.TopicUpdated =
    proto.collector.TopicUpdated(
      Some(
        proto.collector.TopicDescription(
          topicDescription.internal,
          topicDescription.partitions
            .map(tpi => proto.collector.TopicDescription.TopicPartitionInfo(
              tpi.id,
              Some(toPb(tpi.leader)),
              tpi.replicas.map(node => toPb(node)),
              tpi.isr.map(node => toPb(node)))))))

  def toPb(node: Node): proto.collector.Node = proto.collector.Node(node.id, node.host, node.port, Option(node.rack).getOrElse(""))

}
