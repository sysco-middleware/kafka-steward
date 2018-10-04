package no.sysco.middleware.kafka.metadata.collector.topic.internal

import java.time.Duration

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit }
import no.sysco.middleware.kafka.metadata.collector.proto.topic.TopicDescriptionPb.TopicPartitionInfoPb
import no.sysco.middleware.kafka.metadata.collector.proto.topic._
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

      val manager = system.actorOf(TopicManager.props(interval, bootstrapServers, topicEventTopic))

      manager ! TopicEventPb("topic-1", TopicEventPb.Event.TopicCreated(TopicCreatedPb()))
      manager ! TopicEventPb("topic-2", TopicEventPb.Event.TopicCreated(TopicCreatedPb()))
      manager ! TopicEventPb("topic-3", TopicEventPb.Event.TopicCreated(TopicCreatedPb()))

      manager ! ListTopics()

      val topicsV0 = expectMsgType[Topics]
      assert(topicsV0.topicsAndDescription.size == 3)
      assert(topicsV0.topicsAndDescription.count(_._2.isEmpty) == 3)

      manager !
        TopicEventPb(
          "topic-1",
          TopicEventPb.Event.TopicUpdated(
            TopicUpdatedPb(
              Some(
                TopicDescriptionPb(
                  internal = false,
                  List(TopicPartitionInfoPb(0, Seq())))))))

      manager ! ListTopics()
      val topicsV1 = expectMsgType[Topics]
      assert(topicsV1.topicsAndDescription.count(_._2.isEmpty) == 2)
      assert(!topicsV1.topicsAndDescription("topic-1").get.internal)
      assert(topicsV1.topicsAndDescription("topic-1").get.partitions.size == 1)

      manager ! TopicEventPb("topic-2", TopicEventPb.Event.TopicDeleted(TopicDeletedPb()))

      manager ! ListTopics()
      val topicsV2 = expectMsgType[Topics]
      assert(topicsV2.topicsAndDescription.count(_._2.isEmpty) == 1)
      assert(topicsV2.topicsAndDescription.get("topic-2").isEmpty)
    }
  }
}
