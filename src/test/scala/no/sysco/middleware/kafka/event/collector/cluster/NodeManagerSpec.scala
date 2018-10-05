package no.sysco.middleware.kafka.event.collector.cluster

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import no.sysco.middleware.kafka.event.collector.cluster.NodeManager.ListNodes
import no.sysco.middleware.kafka.event.collector.model.Nodes
import no.sysco.middleware.kafka.event.proto.collector.{ Node, NodeCreated, NodeEvent, NodeUpdated }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

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

  "Node Manager" must {
    "should maintain nodes state based on events exchanged" in {
      implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
      implicit val executionContext: ExecutionContext = system.dispatcher

      val eventProducer = TestProbe()

      val manager = system.actorOf(NodeManager.props(eventProducer.ref))

      manager ! NodeEvent(0, NodeEvent.Event.NodeCreated(NodeCreated(Some(Node(0, "localhost", 9092)))))
      manager ! NodeEvent(1, NodeEvent.Event.NodeCreated(NodeCreated(Some(Node(1, "localhost", 9093)))))
      manager ! NodeEvent(2, NodeEvent.Event.NodeCreated(NodeCreated(Some(Node(2, "localhost", 9094)))))

      manager ! ListNodes()

      val topicsV0 = expectMsgType[Nodes]
      assert(topicsV0.nodes.size == 3)

      manager !
        NodeEvent(
          0,
          NodeEvent.Event.NodeUpdated(
            NodeUpdated(
              Some(
                Node(0, "host", 9092)))))

      manager ! ListNodes()
      val topicsV1 = expectMsgType[Nodes]
      assert(topicsV1.nodes.count(n => n._2.host.equals("localhost")) == 2)
      assert(topicsV1.nodes(0).host.equals("host"))
    }
  }
}