package no.sysco.middleware.kafka.event.collector.internal

import no.sysco.middleware.kafka.event.proto
import org.apache.kafka
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class ParserSpec extends FlatSpec {

  "A Parser" should "convert a Kafka Topic Description into a Local Description" in {
    val node = new kafka.common.Node(0, "localhost", 9092)
    val replicas = List(node).asJava
    val topicDescription: kafka.clients.admin.TopicDescription =
      new kafka.clients.admin.TopicDescription(
        "topic",
        false,
        List(new kafka.common.TopicPartitionInfo(0, node, replicas, replicas)).asJava)
    val description = Parser.fromKafka(topicDescription)
    assert(!description.internal)
    assert(description.partitions.size == 1)
  }

  it should "convert a PB Topic Description into a Local Description and vice-versa" in {
    val nodePb = proto.collector.Node(0, "localhost", 9092, null)
    val node = Parser.fromPb(nodePb)
    assert(node.host.equals("localhost"))
    assert(node.port == 9092)
    assert(node.id == 0)
    val pb = proto.collector.TopicDescription(internal = false, List(proto.collector.TopicDescription.TopicPartitionInfo(0, Some(nodePb), Seq(nodePb), Seq(nodePb))))
    val description = Parser.fromPb("topic", pb)
    assert(!description.internal)
    assert(description.partitions.size == 1)
    val descriptionPb = Parser.toPb(description)
    assert(pb.equals(descriptionPb.topicDescription.get))
  }

}
