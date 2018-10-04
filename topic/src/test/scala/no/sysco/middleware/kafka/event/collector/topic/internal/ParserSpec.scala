package no.sysco.middleware.kafka.event.collector.topic.internal

import no.sysco.middleware.kafka.metadata.collector.proto.topic.TopicDescriptionPb.TopicPartitionInfoPb
import no.sysco.middleware.kafka.metadata.collector.proto.topic.{ NodePb, TopicDescriptionPb }
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common
import org.apache.kafka.common.TopicPartitionInfo
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class ParserSpec extends FlatSpec {

  "A Parser" should "convert a Kafka Topic Description into a Local Description" in {
    val node = new common.Node(0, "localhost", 9092)
    val topicDescription: TopicDescription =
      new TopicDescription(
        "topic",
        false,
        List(new TopicPartitionInfo(0, node, List(node).asJava, List(node).asJava)).asJava)
    val description = Parser.fromKafka(topicDescription)
    assert(!description.internal)
    assert(description.partitions.size == 1)
  }

  it should "convert a PB Topic Description into a Local Description and vice-versa" in {
    val nodePb = NodePb(0, "localhost", 9092)
    val node = Parser.fromPb(nodePb)
    assert(node.host.equals("localhost"))
    assert(node.port == 9092)
    assert(node.id == 0)
    val pb = TopicDescriptionPb(internal = false, List(TopicPartitionInfoPb(0, Seq(nodePb))))
    val description = Parser.fromPb("topic", pb)
    assert(!description.internal)
    assert(description.partitions.size == 1)
    val descriptionPb = Parser.toPb(description)
    assert(pb.equals(descriptionPb.topicDescription.get))
  }

}
