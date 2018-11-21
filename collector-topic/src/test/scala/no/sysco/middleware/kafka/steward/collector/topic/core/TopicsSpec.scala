package no.sysco.middleware.kafka.steward.collector.topic.core

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import no.sysco.middleware.kafka.steward.collector.topic.core.Topics.GetState
import no.sysco.middleware.kafka.steward.collector.topic.core.model._
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.duration._
import scala.language.postfixOps

class TopicsSpec extends TestKit(ActorSystem("TopicsSpec"))
  with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll: Unit = TestKit.shutdownActorSystem(system)

  val clusterId: ClusterId = ClusterId("test")

  "Topics actor" must {
    "maintain Topics state" in {
      val topics = system.actorOf(Topics.props(clusterId, TestProbe().ref))
      // initial empty state.
      topics ! GetState()
      expectMsg(Map())
      // handle updates
      val testTopic0 = Topic("testTopic0", Set())
      topics ! TopicUpdated(clusterId, testTopic0)
      topics ! GetState()
      expectMsg(Map(("testTopic0", testTopic0)))
      val testTopic1 = Topic("testTopic1", Set())
      topics ! TopicUpdated(clusterId, testTopic1)
      val testTopic2 = Topic("testTopic2", Set())
      topics ! TopicUpdated(clusterId, testTopic2)
      topics ! GetState()
      val state0 = expectMsgType[Map[String, Topic]]
      assert(3 == state0.size)
      // handle overrides
      val testTopic0Updated = Topic("testTopic0", Set(Partition(1, Set())))
      topics ! TopicUpdated(clusterId, testTopic0Updated)
      topics ! GetState()
      val state1 = expectMsgType[Map[String, Topic]]
      assert(3 == state1.size)
      assert(state1("testTopic0").equals(testTopic0Updated))
      //handle deleted
      topics ! TopicDeleted(clusterId, "testTopic1")
      topics ! GetState()
      val state2 = expectMsgType[Map[String, Topic]]
      assert(2 == state2.size)
    }

    "propagate new and deleted topics" in {
      val probe = TestProbe()
      // initial state
      val topics = system.actorOf(Topics.props(clusterId, probe.ref))
      val testTopic0 = Topic("testTopic0", Set())
      topics ! TopicUpdated(clusterId, testTopic0)
      val testTopic1 = Topic("testTopic1", Set())
      topics ! TopicUpdated(clusterId, testTopic1)
      val testTopic2 = Topic("testTopic2", Set())
      topics ! TopicUpdated(clusterId, testTopic2)
      // when receiving same list of topics
      topics ! TopicsCollected(List("testTopic0", "testTopic1", "testTopic2"))
      probe.expectNoMessage(100 millis)
      // when receiving list with a new topic
      topics ! TopicsCollected(List("testTopic0", "testTopic1", "testTopic2", "testTopic3"))
      val topicCreated = probe.expectMsgType[TopicCreated]
      assert(topicCreated.name.equals("testTopic3"))
      // when receiving list with a topic deleted
      topics ! TopicsCollected(List("testTopic0", "testTopic2", "testTopic3"))
      val topicDeleted = probe.expectMsgType[TopicDeleted]
      assert(topicDeleted.name.equals("testTopic1"))
    }
  }
}
