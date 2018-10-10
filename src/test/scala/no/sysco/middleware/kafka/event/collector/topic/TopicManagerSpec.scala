package no.sysco.middleware.kafka.event.collector.topic

import java.time.Duration

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import no.sysco.middleware.kafka.event.collector.internal.EventRepository.{ CollectTopics, DescribeConfig, DescribeTopic, ResourceType }
import no.sysco.middleware.kafka.event.collector.model.{ TopicDescription, _ }
import no.sysco.middleware.kafka.event.collector.topic.TopicManager.ListTopics
import no.sysco.middleware.kafka.event.proto
import no.sysco.middleware.kafka.event.proto.collector.{ TopicCreated, TopicDeleted, TopicEvent, TopicUpdated }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class TopicManagerSpec
  extends TestKit(ActorSystem("test-topic-manager"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  "TopicManager" when {
    "started" should {
      "request to collect topics to repository in a regular frequency" in {
        val eventRepository = TestProbe()
        val eventProducer = TestProbe()

        // once actor is started, a collect topics message has to be published
        system.actorOf(
          TopicManager.props(
            pollInterval = Duration.ofSeconds(1),
            eventRepository = eventRepository.ref,
            eventProducer = eventProducer.ref))

        // and frequently requested
        eventRepository.expectMsgType[CollectTopics](1.5 seconds)
        eventRepository.expectMsgType[CollectTopics](1.5 seconds)
        eventRepository.expectMsgType[CollectTopics](1.5 seconds)
      }
    }

    "receiving events" should {
      "maintain list of current topics updated" in {
        val eventRepository = TestProbe()
        val eventProducer = TestProbe()

        val manager =
          system.actorOf(
            TopicManager.props(
              pollInterval = Duration.ofDays(1),
              eventRepository = eventRepository.ref,
              eventProducer = eventProducer.ref))

        // 3 topics are created
        manager ! TopicEvent("topic-1", TopicEvent.Event.TopicCreated(TopicCreated()))
        manager ! TopicEvent("topic-2", TopicEvent.Event.TopicCreated(TopicCreated()))
        manager ! TopicEvent("topic-3", TopicEvent.Event.TopicCreated(TopicCreated()))

        // then, 3 topics should be ask to be described
        eventRepository.expectMsg(DescribeTopic("topic-1"))
        eventRepository.expectMsg(DescribeTopic("topic-2"))
        eventRepository.expectMsg(DescribeTopic("topic-3"))

        // and, current state should not have 3 topics, as empty topics are not returned
        manager ! ListTopics()

        val topicsV0 = expectMsgType[Topics]
        assert(topicsV0.topics.isEmpty)

        // if a topic is updated
        manager !
          TopicEvent(
            "topic-1",
            TopicEvent.Event.TopicUpdated(
              TopicUpdated(
                Some(
                  proto.collector.TopicDescription(
                    internal = false,
                    List(proto.collector.TopicDescription.TopicPartitionInfo(0, Some(proto.collector.Node(0, "localhost", 9092, "1")))))))))

        manager !
          TopicEvent(
            "topic-2",
            TopicEvent.Event.TopicUpdated(
              TopicUpdated(
                Some(
                  proto.collector.TopicDescription(
                    internal = false,
                    List(proto.collector.TopicDescription.TopicPartitionInfo(0, Some(proto.collector.Node(0, "localhost", 9092, "1")))))))))

        // then, current state should reflect topic updated
        manager ! ListTopics()
        val topicsV1 = expectMsgType[Topics]
        assert(topicsV1.topics.size == 2)
        assert(!topicsV1.topics.find(s => s.name.equals("topic-1")).get.description.internal)
        assert(topicsV1.topics.find(s => s.name.equals("topic-1")).get.description.partitions.size == 1)

        // finally, if topic is deleted
        manager ! TopicEvent("topic-2", TopicEvent.Event.TopicDeleted(TopicDeleted()))

        // then, current state should have just 2 topics.
        manager ! ListTopics()
        val topicsV2 = expectMsgType[Topics]
        assert(topicsV2.topics.size == 1)
      }
    }

    "includeInternalTopics is true" should {
      "describe internal topic" in {
        val eventRepositoryProbe = TestProbe()
        val eventProducerProbe = TestProbe()

        val manager =
          system.actorOf(
            TopicManager.props(
              pollInterval = Duration.ofSeconds(100),
              eventRepository = eventRepositoryProbe.ref,
              eventProducer = eventProducerProbe.ref))

        // describe 2 internal
        manager ! TopicDescribed(("topic-1", TopicDescription(internal = false, List.empty)))
        eventRepositoryProbe.expectMsg(DescribeConfig(ResourceType.Topic, "topic-1"))
        eventRepositoryProbe.reply(ConfigDescribed(Config()))
        eventProducerProbe.expectMsgType[TopicEvent]
        manager ! TopicDescribed(("topic-2", TopicDescription(internal = false, List.empty)))
        eventRepositoryProbe.expectMsg(DescribeConfig(ResourceType.Topic, "topic-2"))
        eventRepositoryProbe.reply(ConfigDescribed(Config()))
        eventProducerProbe.expectMsgType[TopicEvent]
        // describe 1 NOT internal
        manager ! TopicDescribed(("topic-3", TopicDescription(internal = true, List.empty)))
        eventRepositoryProbe.expectMsg(DescribeConfig(ResourceType.Topic, "topic-3"))
        eventRepositoryProbe.reply(ConfigDescribed(Config()))
        eventProducerProbe.expectMsgType[TopicEvent]
      }
    }

    "includeInternalTopics=false" should {
      "not describe internal topic" in {
        val eventRepositoryProbe = TestProbe()
        val eventProducerProbe = TestProbe()

        val manager =
          system.actorOf(
            TopicManager.props(
              pollInterval = Duration.ofSeconds(100),
              eventRepository = eventRepositoryProbe.ref,
              eventProducer = eventProducerProbe.ref,
              includeInternalTopics = false))

        // describe 2 internal
        manager ! TopicDescribed(("topic-1", TopicDescription(internal = false, List.empty)))
        eventRepositoryProbe.expectMsg(DescribeConfig(ResourceType.Topic, "topic-1"))
        eventRepositoryProbe.reply(ConfigDescribed(Config()))
        eventProducerProbe.expectMsgType[TopicEvent]
        manager ! TopicDescribed(("topic-2", TopicDescription(internal = false, List.empty)))
        eventRepositoryProbe.expectMsg(DescribeConfig(ResourceType.Topic, "topic-2"))
        eventRepositoryProbe.reply(ConfigDescribed(Config()))
        eventProducerProbe.expectMsgType[TopicEvent]
        // describe 1 NOT internal
        manager ! TopicDescribed(("topic-3", TopicDescription(internal = true, List.empty)))
        eventRepositoryProbe.expectNoMessage()
        eventProducerProbe.expectNoMessage()
      }
    }

    "if blacklist is set and topic included is collected" should {
      "not consider that topic for evaluation" in {
        val eventRepositoryProbe = TestProbe()
        val eventProducerProbe = TestProbe()

        val manager =
          system.actorOf(
            TopicManager.props(
              pollInterval = Duration.ofSeconds(100),
              includeInternalTopics = false,
              blacklistTopics = List("topic-3"),
              eventRepository = eventRepositoryProbe.ref,
              eventProducer = eventProducerProbe.ref))

        // if topic-3 is collected
        manager ! TopicsCollected(List("topic-1", "topic-2", "topic-3"))

        // then, event for topic-3 should not be produced
        val event1 = eventProducerProbe.expectMsgType[TopicEvent]
        assert(event1.name.equals("topic-1"))
        assert(event1.event.isTopicCreated)
        val event2 = eventProducerProbe.expectMsgType[TopicEvent]
        assert(event2.name.equals("topic-2"))
        assert(event2.event.isTopicCreated)
        eventProducerProbe.expectNoMessage(3 seconds)
      }
    }

    "if whitelist is set and topic not included is collected" should {
      "not consider that topic for evaluation" in {
        val eventRepositoryProbe = TestProbe()
        val eventProducerProbe = TestProbe()

        val manager =
          system.actorOf(
            TopicManager.props(
              pollInterval = Duration.ofSeconds(100),
              includeInternalTopics = false,
              whitelistTopics = List("topic-3"),
              eventRepository = eventRepositoryProbe.ref,
              eventProducer = eventProducerProbe.ref))

        // if topic-3 is collected
        manager ! TopicsCollected(List("topic-1", "topic-2", "topic-3"))

        // then, only event for topic-3 should be produced
        val event1 = eventProducerProbe.expectMsgType[TopicEvent]
        assert(event1.name.equals("topic-3"))
        assert(event1.event.isTopicCreated)
        eventProducerProbe.expectNoMessage(3 seconds)
      }
    }

    "if blacklist and whitelist is set and topic is included in both" should {
      "not consider that topic for evaluation as blacklist has more weight" in {
        val eventRepositoryProbe = TestProbe()
        val eventProducerProbe = TestProbe()

        val manager =
          system.actorOf(
            TopicManager.props(
              pollInterval = Duration.ofSeconds(100),
              includeInternalTopics = false,
              blacklistTopics = List("topic-3"),
              whitelistTopics = List("topic-3"),
              eventRepository = eventRepositoryProbe.ref,
              eventProducer = eventProducerProbe.ref))

        // if topic-3 is collected
        manager ! TopicsCollected(List("topic-1", "topic-2", "topic-3"))

        // then, no event should be produced
        eventProducerProbe.expectNoMessage(3 seconds)
      }
    }

    "topicsCollected do not include current topic" should {
      "evaluate that a topic has been deleted" in {
        val eventRepositoryProbe = TestProbe()
        val eventProducerProbe = TestProbe()

        val manager =
          system.actorOf(
            TopicManager.props(
              pollInterval = Duration.ofSeconds(100),
              includeInternalTopics = false,
              eventRepository = eventRepositoryProbe.ref,
              eventProducer = eventProducerProbe.ref))

        // collect 3 topics
        manager !
          TopicEvent(
            "topic-1",
            TopicEvent.Event.TopicUpdated(
              TopicUpdated(
                Some(
                  proto.collector.TopicDescription(
                    internal = false,
                    List(proto.collector.TopicDescription.TopicPartitionInfo(0, Some(proto.collector.Node(0, "localhost", 9092, "1")))))))))
        manager !
          TopicEvent(
            "topic-2",
            TopicEvent.Event.TopicUpdated(
              TopicUpdated(
                Some(
                  proto.collector.TopicDescription(
                    internal = false,
                    List(proto.collector.TopicDescription.TopicPartitionInfo(0, Some(proto.collector.Node(0, "localhost", 9092, "1")))))))))
        manager !
          TopicEvent(
            "topic-3",
            TopicEvent.Event.TopicUpdated(
              TopicUpdated(
                Some(
                  proto.collector.TopicDescription(
                    internal = false,
                    List(proto.collector.TopicDescription.TopicPartitionInfo(0, Some(proto.collector.Node(0, "localhost", 9092, "1")))))))))

        // if topic-3 is not collected
        manager ! TopicsCollected(List("topic-1", "topic-2"))

        // then, an event to delete the topic-3 should be produced
        val event = eventProducerProbe.expectMsgType[TopicEvent]
        assert(event.name.equals("topic-3"))
        assert(event.event.isTopicDeleted)
      }
    }
  }
}
