package no.sysco.middleware.kafka.steward.collector.cluster

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import no.sysco.middleware.kafka.steward.collector.cluster.BrokerManager.ListBrokers
import no.sysco.middleware.kafka.steward.collector.internal.EventRepository.{ DescribeConfig, ResourceType }
import no.sysco.middleware.kafka.steward.collector.model._
import no.sysco.middleware.kafka.steward.proto.collector.{ BrokerCreated, BrokerEvent, BrokerUpdated }
import no.sysco.middleware.kafka.steward.proto
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.ExecutionContext

class BrokerManagerSpec
  extends TestKit(ActorSystem("test-broker-manager"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  "BrokerManager" when {

    "nodes are described" should {
      "publish creation events when nodes are not in state" in {
        val eventRepository = TestProbe()
        val eventProducer = TestProbe()

        val manager = system.actorOf(BrokerManager.props(eventRepository.ref, eventProducer.ref))

        manager ! NodesDescribed(List(Node(0, "localhost", 9092)))
        eventRepository.expectMsg(DescribeConfig(ResourceType.Broker, "0"))
        eventRepository.reply(ConfigDescribed(Config()))

        val brokerEvent = eventProducer.expectMsgType[BrokerEvent]
        assert(brokerEvent.event.isBrokerCreated)
        assert(brokerEvent.id.equals("0"))
      }

      "publish update events when nodes are in state" in {
        val eventRepository = TestProbe()
        val eventProducer = TestProbe()

        val manager = system.actorOf(BrokerManager.props(eventRepository.ref, eventProducer.ref))

        manager ! BrokerEvent("0", BrokerEvent.Event.BrokerCreated(BrokerCreated(Some(proto.collector.Node(0, "localhost", 9092)))))
        manager ! BrokerEvent("1", BrokerEvent.Event.BrokerCreated(BrokerCreated(Some(proto.collector.Node(1, "localhost", 9093)))))
        manager ! BrokerEvent("2", BrokerEvent.Event.BrokerCreated(BrokerCreated(Some(proto.collector.Node(2, "localhost", 9094)))))

        manager ! NodesDescribed(List(Node(1, "host", 9092)))
        eventRepository.expectMsg(DescribeConfig(ResourceType.Broker, "1"))
        eventRepository.reply(ConfigDescribed(Config(Map(("a", "b")))))

        val brokerEvent = eventProducer.expectMsgType[BrokerEvent]
        assert(brokerEvent.event.isBrokerUpdated)
        assert(brokerEvent.id.equals("1"))
      }
    }

    "node events happen" should {
      "maintain nodes state" in {

        val eventRepository = TestProbe()
        val eventProducer = TestProbe()

        val manager = system.actorOf(BrokerManager.props(eventRepository.ref, eventProducer.ref))

        manager ! BrokerEvent("0", BrokerEvent.Event.BrokerCreated(BrokerCreated(Some(proto.collector.Node(0, "localhost", 9092)))))
        manager ! BrokerEvent("1", BrokerEvent.Event.BrokerCreated(BrokerCreated(Some(proto.collector.Node(1, "localhost", 9093)))))
        manager ! BrokerEvent("2", BrokerEvent.Event.BrokerCreated(BrokerCreated(Some(proto.collector.Node(2, "localhost", 9094)))))

        manager ! ListBrokers()

        val brokersV0 = expectMsgType[Brokers]
        assert(brokersV0.brokers.size == 3)

        manager !
          BrokerEvent(
            "0",
            BrokerEvent.Event.BrokerUpdated(
              BrokerUpdated(Some(proto.collector.Node(0, "host", 9092)))))

        manager ! ListBrokers()
        val brokersV1 = expectMsgType[Brokers]
        assert(brokersV1.brokers.count(n => n.node.host.equals("localhost")) == 2)
        assert(brokersV1.brokers.find(b => b.id.equals("0")).get.node.host.equals("host")) //FIXME
      }
    }
  }
}