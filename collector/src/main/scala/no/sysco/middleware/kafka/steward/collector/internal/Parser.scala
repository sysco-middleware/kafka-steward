package no.sysco.middleware.kafka.steward.collector.internal

import no.sysco.middleware.kafka.steward.collector.model.{ Config, Node, Partition, TopicDescription }
import no.sysco.middleware.kafka.steward.proto
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
            partition.replicas().asScala.toList.map(node => fromKafka(node)),
            partition.isr().asScala.toList.map(node => fromKafka(node)))).toList)

  def fromKafka(node: common.Node): Node = Node(node.id(), node.host(), node.port(), Option(node.rack()))

  def fromKafka(config: admin.Config): Config = Config(config.entries().asScala.map(s => (s.name(), s.value())).toMap)

  def fromPb(name: String, topicDescription: proto.collector.TopicDescription): TopicDescription =
    TopicDescription(
      topicDescription.internal,
      topicDescription.topicPartitions.toList
        .map(tp =>
          Partition(
            tp.partition,
            fromPb(tp.leader.get),
            tp.replicas.toList.map(rep => fromPb(rep)),
            tp.isr.toList.map(rep => fromPb(rep)))))

  def fromPb(node: proto.collector.Node): Node =
    Node(node.id, node.host, node.port, node.rack match {
      case null => None
      case s if s.isEmpty => None
      case s => Some(s)
    })

  def fromPb(configOption: Option[proto.collector.Config]): Config =
    configOption match {
      case Some(config) => Config(config.entries)
      case None => Config()
    }

  def toPb(topicDescription: TopicDescription, config: Config): proto.collector.TopicUpdated =
    proto.collector.TopicUpdated(
      Some(
        proto.collector.TopicDescription(
          topicDescription.internal,
          topicDescription.partitions
            .map(tpi => proto.collector.TopicDescription.TopicPartitionInfo(
              tpi.id,
              Some(toPb(tpi.leader)),
              tpi.replicas.map(node => toPb(node)),
              tpi.isr.map(node => toPb(node)))))),
      Some(toPb(config)))

  def toPb(node: Node): proto.collector.Node =
    proto.collector.Node(node.id, node.host, node.port, node.rack.getOrElse(""))

  def toPb(config: Config): proto.collector.Config = proto.collector.Config(config.entries)
}
