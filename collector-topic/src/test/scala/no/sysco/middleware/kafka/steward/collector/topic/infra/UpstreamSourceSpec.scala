package no.sysco.middleware.kafka.steward.collector.topic.infra

import java.util.Properties

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import no.sysco.middleware.kafka.steward.collector.topic.core.model.{TopicDescribed, TopicsCollected}
import no.sysco.middleware.kafka.steward.collector.topic.infra.UpstreamSource.ListTopics
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.JavaConverters._

class UpstreamSourceSpec
  extends TestKit(ActorSystem("UpstreamSourceSpec"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EmbeddedKafka {

  override def afterAll: Unit = TestKit.shutdownActorSystem(system)

  val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  "Upstream source" must {
    "propagate all topics when filters are empty" in {
      withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
        // Initial state.
        val adminConfigs = new Properties()
        adminConfigs.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${actualConfig.kafkaPort}")
        val adminClient = AdminClient.create(adminConfigs)
        adminClient.createTopics(
          List(
            new NewTopic("testTopic0", 1, 1),
            new NewTopic("testTopic1", 1, 1),
            new NewTopic("testTopic2", 1, 1),
            new NewTopic("testTopic3", 1, 1),
          ).asJava)
          .all().get()
        // When listing is required
        val testProbe = TestProbe()
        val repo = system.actorOf(UpstreamSource.props(adminClient, testProbe.ref))
        repo ! ListTopics()
        // Then
        val topicsCollected = testProbe.expectMsgType[TopicsCollected]
        assert(4 == topicsCollected.topics.size)
        val topicDescribed0 = testProbe.expectMsgType[TopicDescribed]
        assert(topicDescribed0.name.startsWith("testTopic"))
        val topicDescribed1 = testProbe.expectMsgType[TopicDescribed]
        assert(topicDescribed1.name.startsWith("testTopic"))
        val topicDescribed2 = testProbe.expectMsgType[TopicDescribed]
        assert(topicDescribed2.name.startsWith("testTopic"))
        val topicDescribed3 = testProbe.expectMsgType[TopicDescribed]
        assert(topicDescribed3.name.startsWith("testTopic"))
        assert(topicDescribed3.topic.config.entries.nonEmpty)
        testProbe.expectNoMessage()

        // When listing is requested with filters
        val testProbe0 = TestProbe()
        val repo0 = system.actorOf(
          UpstreamSource.props(
            adminClient,
            testProbe0.ref,
            whitelist = Set("testTopic1", "testTopic2"),
            blacklist = Set("testTopic3")))
        repo0 ! ListTopics()
        val topicsCollected0 = testProbe0.expectMsgType[TopicsCollected]
        assert(2 == topicsCollected0.topics.size)
        assert(topicsCollected0.topics.contains("testTopic1"))
        assert(topicsCollected0.topics.contains("testTopic2"))

        val testProbe1 = TestProbe()
        val repo1 = system.actorOf(
          UpstreamSource.props(
            adminClient,
            testProbe1.ref,
            blacklist = Set("testTopic3")))
        repo1 ! ListTopics()
        val topicsCollected1 = testProbe1.expectMsgType[TopicsCollected]
        assert(3 == topicsCollected1.topics.size)
        assert(topicsCollected1.topics.contains("testTopic0"))
        assert(topicsCollected1.topics.contains("testTopic1"))
        assert(topicsCollected1.topics.contains("testTopic2"))
      }
    }
  }

}
