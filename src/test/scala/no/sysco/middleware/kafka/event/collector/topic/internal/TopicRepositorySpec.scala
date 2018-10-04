package no.sysco.middleware.kafka.event.collector.topic.internal

import java.util.Properties

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import no.sysco.middleware.kafka.event.collector.model._

import scala.collection.JavaConverters._

class TopicRepositorySpec
  extends TestKit(ActorSystem("test-topic-repository"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EmbeddedKafka {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)


  "Topic Repository" must {
    "send back topic and description" in {
      withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
        val adminConfigs = new Properties()
        adminConfigs.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${actualConfig.kafkaPort}")
        val adminClient = AdminClient.create(adminConfigs)

        adminClient
          .createTopics(
            List(
              new NewTopic("test-1", 1, 1),
              new NewTopic("test-2", 1, 1),
              new NewTopic("test-3", 1, 1),
            ).asJava).all().get()

        val repo = system.actorOf(TopicRepository.props(adminClient))

        repo ! CollectTopics()
        val topicsCollected: TopicsCollected = expectMsgType[TopicsCollected]
        assert(topicsCollected.names.size == 3)
        assert(topicsCollected.names.contains("test-1"))
        assert(topicsCollected.names.contains("test-2"))
        assert(topicsCollected.names.contains("test-3"))

        repo ! DescribeTopic("test-1")
        val topicDescribed: TopicDescribed = expectMsgType[TopicDescribed]
        assert(topicDescribed.topicAndDescription._1.equals("test-1"))
      }
    }
  }
}
