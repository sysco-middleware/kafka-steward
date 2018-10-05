package no.sysco.middleware.kafka.event.collector.topic

import java.time.Duration

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.proto
import no.sysco.middleware.kafka.event.proto.collector.TopicDescription.TopicPartitionInfo
import no.sysco.middleware.kafka.event.proto.collector._
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.ExecutionContext

class TopicManagerSpec
  extends TestKit(ActorSystem("test-topic-manager"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "Topic Manager" must {
    "should have a new topic stored when there is a existing topic" in {
      implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
      implicit val executionContext: ExecutionContext = system.dispatcher

      val bootstrapServers = s"localhost:0"
      val topicEventTopic = "__topic"
      val interval = Duration.ofSeconds(100)

      val eventRepository = TestProbe()
      val eventProducer = TestProbe()

      val manager = system.actorOf(TopicManager.props(interval, eventRepository.ref, eventProducer.ref))

      manager ! TopicEvent("topic-1", TopicEvent.Event.TopicCreated(TopicCreated()))
      manager ! TopicEvent("topic-2", TopicEvent.Event.TopicCreated(TopicCreated()))
      manager ! TopicEvent("topic-3", TopicEvent.Event.TopicCreated(TopicCreated()))

      manager ! ListTopics()

      val topicsV0 = expectMsgType[Topics]
      assert(topicsV0.topicsAndDescription.size == 3)
      assert(topicsV0.topicsAndDescription.count(_._2.isEmpty) == 3)

      manager !
        TopicEvent(
          "topic-1",
          TopicEvent.Event.TopicUpdated(
            TopicUpdated(
              Some(
                TopicDescription(
                  internal = false,
                  List(TopicPartitionInfo(0, Some(proto.collector.Node(0, "localhost", 9092, "1")))))))))

      manager ! ListTopics()
      val topicsV1 = expectMsgType[Topics]
      assert(topicsV1.topicsAndDescription.count(_._2.isEmpty) == 2)
      assert(!topicsV1.topicsAndDescription("topic-1").get.internal)
      assert(topicsV1.topicsAndDescription("topic-1").get.partitions.size == 1)

      manager ! TopicEvent("topic-2", TopicEvent.Event.TopicDeleted(TopicDeleted()))

      manager ! ListTopics()
      val topicsV2 = expectMsgType[Topics]
      assert(topicsV2.topicsAndDescription.count(_._2.isEmpty) == 1)
      assert(topicsV2.topicsAndDescription.get("topic-2").isEmpty)
    }
  }
}
