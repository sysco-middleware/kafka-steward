package no.sysco.middleware.kafka.event.collector.cluster

import java.time.Duration

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import no.sysco.middleware.kafka.event.collector.cluster.ClusterManager.GetCluster
import no.sysco.middleware.kafka.event.collector.model.{ Cluster, Parser }
import no.sysco.middleware.kafka.event.proto.collector._
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.ExecutionContext

class ClusterManagerSpec
  extends TestKit(ActorSystem("cluster-topic-manager"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "Cluster Manager" must {
    "should maintain cluster state based on events exchanged" in {
      implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
      implicit val executionContext: ExecutionContext = system.dispatcher

      val interval = Duration.ofSeconds(100)

      val eventProducer = TestProbe()
      val eventRepository = TestProbe()

      val manager = system.actorOf(ClusterManager.props(interval, eventRepository.ref, eventProducer.ref))

      val node = Node(0, "localhost", 9092)
      manager ! ClusterEvent("cluster-1", ClusterEvent.Event.ClusterCreated(ClusterCreated(Some(node))))

      manager ! GetCluster()

      val topicsV0 = expectMsgType[Option[Cluster]]
      assert(topicsV0.get.id.equals("cluster-1"))
      assert(topicsV0.get.controller.contains(Parser.fromPb(node)))

      val otherNode = Node(1, "other-host", 9092)
      manager !
        ClusterEvent(
          "cluster-1",
          ClusterEvent.Event.ClusterUpdated(
            ClusterUpdated(Some(otherNode))))

      manager ! GetCluster()
      val topicsV1 = expectMsgType[Option[Cluster]]
      assert(topicsV1.get.controller.contains(Parser.fromPb(otherNode)))
    }
  }
}