package no.sysco.middleware.kafka.event.collector.internal

import java.util.Properties

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import no.sysco.middleware.kafka.event.collector.internal.EventRepository._
import no.sysco.middleware.kafka.event.collector.model._
import org.apache.kafka.clients.admin
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.common.config.ConfigResource
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class EventRepositorySpec
  extends TestKit(ActorSystem("test-event-repository"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EmbeddedKafka {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  "Event Repository" must {
    "send back cluster, nodes, topic and description" in {
      withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
        val adminConfigs = new Properties()
        adminConfigs.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${actualConfig.kafkaPort}")
        val adminClient = AdminClient.create(adminConfigs)

        val repo = system.actorOf(EventRepository.props(adminClient))
        repo ! DescribeCluster()
        val clusterDescribed: ClusterDescribed = expectMsgType[ClusterDescribed]
        assert(!clusterDescribed.id.isEmpty)
        assert(clusterDescribed.controller.isDefined)
        assert(clusterDescribed.nodes.size == 1)

        adminClient
          .createTopics(
            List(
              new NewTopic("test-1", 1, 1),
              new NewTopic("test-2", 1, 1),
              new NewTopic("test-3", 1, 1),
            ).asJava).all().get()

        repo ! CollectTopics()
        val topicsCollected: TopicsCollected = expectMsgType[TopicsCollected]
        assert(topicsCollected.names.size == 3)
        assert(topicsCollected.names.contains("test-1"))
        assert(topicsCollected.names.contains("test-2"))
        assert(topicsCollected.names.contains("test-3"))

        repo ! DescribeTopic("test-1")
        val topicDescribed: TopicDescribed = expectMsgType[TopicDescribed]
        assert(topicDescribed.topicAndDescription._1.equals("test-1"))

        repo ! DescribeConfig(ResourceType.Topic, "test-1")
        val configDescribedV0: ConfigDescribed = expectMsgType[ConfigDescribed]
        assert(configDescribedV0.config.entries.nonEmpty)

        val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "test-1")
        adminClient.alterConfigs(
          Map((configResource,
            new admin.Config(
              List(new admin.ConfigEntry("cleanup.policy", "compact")).asJava))).asJava)
          .all()
          .get()

        repo ! DescribeConfig(ResourceType.Topic, "test-1")
        val configDescribed: ConfigDescribed = expectMsgType[ConfigDescribed]
        assert(configDescribed.config.entries.getOrElse("cleanup.policy","").equals("compact"))

        repo ! DescribeConfig(ResourceType.Broker, "0")
        val configDescribedV2: ConfigDescribed = expectMsgType[ConfigDescribed](5 seconds)
        assert(configDescribedV2.config.entries.nonEmpty)
      }
    }
  }
}
