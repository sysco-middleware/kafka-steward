package no.sysco.middleware.kafka.event.collector.internal

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import no.sysco.middleware.kafka.event.proto.collector.{ CollectorEvent, TopicCreated, TopicEvent }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class EventProducerConsumerSpec
  extends TestKit(ActorSystem("test-event-producer-consumer", ConfigFactory.parseString(
    """
      akka.kafka.consumer {
        poll-interval = 100ms
        kafka-clients {
          enable-auto-commit = false
          auto-offset-reset = earliest
        }
      }
  """)))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with EmbeddedKafka {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  "Event Producer and Consumer" must {
    "send and receive Events from/to a Kafka Topic" in {
      withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
        implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
        implicit val executionContext: ExecutionContext = system.dispatcher
        val probe = TestProbe()
        val bootstrapServers = s"localhost:${actualConfig.kafkaPort}"
        val topicEventTopic = "__collector"

        system.actorOf(EventConsumer.props(probe.ref, bootstrapServers, topicEventTopic))

        val eventProducer = system.actorOf(EventProducer.props(bootstrapServers, topicEventTopic))

        val event =
          CollectorEvent(
            CollectorEvent.EntityType.TOPIC,
            "test",
            CollectorEvent.Value.TopicEvent(
              TopicEvent(
                "test",
                TopicEvent.Event.TopicCreated(TopicCreated()))))
        eventProducer ! event

        val eventReceived = probe.expectMsgType[CollectorEvent](10 seconds)
        assert(event.equals(eventReceived))
      }
    }
  }
}
