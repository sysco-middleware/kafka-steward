package no.sysco.middleware.kafka.event.collector.cluster

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import no.sysco.middleware.kafka.event.collector.cluster.NodeManager.ListNodes
import no.sysco.middleware.kafka.event.collector.model.{Node, Nodes, NodesDescribed}
import no.sysco.middleware.kafka.event.proto
import no.sysco.middleware.kafka.event.proto.collector.{NodeCreated, NodeEvent, NodeUpdated}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext

class NodeManagerSpec
  extends TestKit(ActorSystem("node-topic-manager"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  "NodeManager" when {

    "nodes are described" should {
      "publish creation events when nodes are not in state" in {
        val eventProducer = TestProbe()

        val manager = system.actorOf(NodeManager.props(eventProducer.ref))

        manager ! NodesDescribed(List(Node(0, "localhost", 9092)))

        val nodeEvent = eventProducer.expectMsgType[NodeEvent]
        assert(nodeEvent.event.isNodeCreated)
        assert(nodeEvent.id.equals(0))
      }
      "publish update events when nodes are in state" in {
        val eventProducer = TestProbe()

        val manager = system.actorOf(NodeManager.props(eventProducer.ref))

        manager ! NodeEvent(0, NodeEvent.Event.NodeCreated(NodeCreated(Some(proto.collector.Node(0, "localhost", 9092)))))
        manager ! NodeEvent(1, NodeEvent.Event.NodeCreated(NodeCreated(Some(proto.collector.Node(1, "localhost", 9093)))))
        manager ! NodeEvent(2, NodeEvent.Event.NodeCreated(NodeCreated(Some(proto.collector.Node(2, "localhost", 9094)))))

        manager ! NodesDescribed(List(Node(1, "host", 9092)))

        val nodeEvent = eventProducer.expectMsgType[NodeEvent]
        assert(nodeEvent.event.isNodeUpdated)
        assert(nodeEvent.id.equals(1))
      }
    }

    "node events happen" should {
      "maintain nodes state" in {

        val eventProducer = TestProbe()

        val manager = system.actorOf(NodeManager.props(eventProducer.ref))

        manager ! NodeEvent(0, NodeEvent.Event.NodeCreated(NodeCreated(Some(proto.collector.Node(0, "localhost", 9092)))))
        manager ! NodeEvent(1, NodeEvent.Event.NodeCreated(NodeCreated(Some(proto.collector.Node(1, "localhost", 9093)))))
        manager ! NodeEvent(2, NodeEvent.Event.NodeCreated(NodeCreated(Some(proto.collector.Node(2, "localhost", 9094)))))

        manager ! ListNodes()

        val topicsV0 = expectMsgType[Nodes]
        assert(topicsV0.nodes.size == 3)

        manager !
          NodeEvent(
            0,
            NodeEvent.Event.NodeUpdated(
              NodeUpdated(Some(proto.collector.Node(0, "host", 9092)))))

        manager ! ListNodes()
        val topicsV1 = expectMsgType[Nodes]
        assert(topicsV1.nodes.count(n => n._2.host.equals("localhost")) == 2)
        assert(topicsV1.nodes("0").host.equals("host"))
      }
    }
  }
}