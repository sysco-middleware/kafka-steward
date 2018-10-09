package no.sysco.middleware.kafka.event.collector.cluster

import java.time.Duration

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import no.sysco.middleware.kafka.event.collector.cluster.ClusterManager.GetCluster
import no.sysco.middleware.kafka.event.collector.internal.EventRepository.DescribeCluster
import no.sysco.middleware.kafka.event.collector.internal.Parser
import no.sysco.middleware.kafka.event.collector.model.{Cluster, ClusterDescribed}
import no.sysco.middleware.kafka.event.proto.collector._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ClusterManagerSpec
  extends TestKit(ActorSystem("cluster-topic-manager"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  "ClusterManager" when {
    "started" should {
      "request to describe cluster to repository in a regular frequency" in {
        val eventRepository = TestProbe()
        val eventProducer = TestProbe()

        system.actorOf(
          ClusterManager.props(
            pollInterval = Duration.ofSeconds(1),
            eventRepository = eventRepository.ref,
            eventProducer = eventProducer.ref))

        // and frequently requested
        eventRepository.expectMsgType[DescribeCluster](1.5 seconds)
        eventRepository.expectMsgType[DescribeCluster](1.5 seconds)
        eventRepository.expectMsgType[DescribeCluster](1.5 seconds)
      }
    }

    "receive events" should {
      "maintain current state of the cluster" in {
        val eventProducer = TestProbe()
        val eventRepository = TestProbe()

        val manager =
          system.actorOf(
            ClusterManager.props(
              pollInterval = Duration.ofSeconds(100),
              eventRepository.ref,
              eventProducer.ref))

        // when cluster is created
        val node = Node(0, "localhost", 9092)
        manager ! ClusterEvent("cluster-1", ClusterEvent.Event.ClusterCreated(ClusterCreated(Some(node))))

        // then, state should contain cluster created
        manager ! GetCluster()
        val topicsV0 = expectMsgType[Option[Cluster]]
        assert(topicsV0.get.id.equals("cluster-1"))
        assert(topicsV0.get.controller.contains(Parser.fromPb(node)))

        // when cluster is updated
        val otherNode = Node(1, "other-host", 9092)
        manager !
          ClusterEvent(
            "cluster-1",
            ClusterEvent.Event.ClusterUpdated(
              ClusterUpdated(Some(otherNode))))

        // then, state should be updated
        manager ! GetCluster()
        val topicsV1 = expectMsgType[Option[Cluster]]
        assert(topicsV1.get.controller.contains(Parser.fromPb(otherNode)))
      }
    }

    "when cluster described" should {
      "publish cluster created event when no state" in {
        val eventProducer = TestProbe()
        val eventRepository = TestProbe()

        val manager =
          system.actorOf(
            ClusterManager.props(
              pollInterval = Duration.ofSeconds(100),
              eventRepository.ref,
              eventProducer.ref))

        manager ! ClusterDescribed("cluster-1", Option.empty, List.empty)

        val clusterEvent = eventRepository.expectMsgType[ClusterEvent]
        assert(clusterEvent.event.isClusterCreated)
      }
      "publish cluster updated event when existing state" in {
        val eventProducer = TestProbe()
        val eventRepository = TestProbe()

        val manager =
          system.actorOf(
            ClusterManager.props(
              pollInterval = Duration.ofSeconds(100),
              eventRepository.ref,
              eventProducer.ref))

        // when cluster is created
        val node = Node(0, "localhost", 9092)
        manager ! ClusterEvent("cluster-1", ClusterEvent.Event.ClusterCreated(ClusterCreated(Some(node))))

        manager ! ClusterDescribed("cluster-1", Option.empty, List())

        val clusterEvent = eventRepository.expectMsgType[ClusterEvent]
        assert(clusterEvent.event.isClusterUpdated)
      }
    }
  }
}