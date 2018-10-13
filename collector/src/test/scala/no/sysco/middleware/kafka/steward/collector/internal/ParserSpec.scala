package no.sysco.middleware.kafka.steward.collector.internal

import no.sysco.middleware.kafka.steward.collector.model.Config
import org.apache.kafka
import org.apache.kafka.clients.admin.ConfigEntry
import org.scalatest.FlatSpec
import no.sysco.middleware.kafka.steward.proto

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
    val nodePb = proto.collector.Node(0, "localhost", 9092)
    val node = Parser.fromPb(nodePb)
    assert(node.host.equals("localhost"))
    assert(node.port == 9092)
    assert(node.id == 0)
    val pb = proto.collector.TopicDescription(internal = false, List(proto.collector.TopicDescription.TopicPartitionInfo(0, Some(nodePb), Seq(nodePb), Seq(nodePb))))
    val description = Parser.fromPb("topic", pb)
    assert(!description.internal)
    assert(description.partitions.size == 1)
    val descriptionPb = Parser.toPb(description, Config())
    assert(pb.equals(descriptionPb.topicDescription.get))
  }

  it should "convert a PB Config into a Local Config and vice-versa" in {
    val configPb =
      proto.collector.Config(
        Seq(
          proto.collector.Config.Entry("k1", "v1"),
          proto.collector.Config.Entry("k2", "v2"),
          proto.collector.Config.Entry("k3", "v3"),
        ))
    val config = Parser.fromPb(Some(configPb))
    assert(config.entries.size == 3)
    assert(config.entries("k1").equals("v1"))
    assert(config.entries("k2").equals("v2"))
    assert(config.entries("k3").equals("v3"))
    val configPb2 = Parser.toPb(config)
    assert(configPb.equals(configPb2))
  }

  it should "convert a Kafka Config into a Local representation" in {
    val configKafka =
      new kafka.clients.admin.Config(
        List(
          new ConfigEntry("k1", "v1"),
          new ConfigEntry("k2", "v2"),
          new ConfigEntry("k3", "v3"),
        ).asJava)
    val config = Parser.fromKafka(configKafka)
    assert(config.entries.size == 3)
    assert(config.entries("k1").equals("v1"))
    assert(config.entries("k2").equals("v2"))
    assert(config.entries("k3").equals("v3"))
  }

}
