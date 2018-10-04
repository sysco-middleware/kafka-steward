package no.sysco.middleware.kafka.event.collector.topic.internal

import akka.actor.{ Actor, ActorRef, Props }
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ ConsumerSettings, Subscriptions }
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.{ ActorMaterializer, KillSwitches, UniqueKillSwitch }
import com.typesafe.config.Config
import no.sysco.middleware.kafka.metadata.collector.proto.topic.TopicEventPb
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future }

object TopicEventConsumer {

  def props(topicManager: ActorRef, bootstrapServers: String, topicEventTopic: String)(implicit materializer: ActorMaterializer, executionContext: ExecutionContext): Props =
    Props(new TopicEventConsumer(topicManager, bootstrapServers, topicEventTopic))
}

/**
 * Consume Topic events.
 *
 * @param topicManager Reference to Topic Manager, to consume events further.
 */
class TopicEventConsumer(topicManager: ActorRef, bootstrapServers: String, topicEventTopic: String)(implicit materializer: ActorMaterializer, executionContext: ExecutionContext)
  extends Actor {

  val config: Config = context.system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withPollInterval(100 millis)
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId("kafka-metadata-collector-topic-internal-consumer")

  val killSwitch: UniqueKillSwitch =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(topicEventTopic))
      .map(event => {
        println(event)
        event
      })
      .map(record => TopicEventPb.parseFrom(record.value()))
      .map(event => {
        println(event)
        event
      })
      .mapAsync(1)(topicEvent => Future {
        topicManager ! topicEvent
      })
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.ignore)
      .run()

  override def receive: Receive = {
    case "stop" =>
      println("Stopping")
      killSwitch.shutdown()
    case "done" =>
      println("Done")
      context.stop(self)
  }
}
