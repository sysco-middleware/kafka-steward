package no.sysco.middleware.kafka.event.collector.internal

import akka.actor.{ Actor, ActorRef, Props }
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ ConsumerSettings, Subscriptions }
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.{ ActorMaterializer, KillSwitches, UniqueKillSwitch }
import no.sysco.middleware.kafka.event.proto.collector.CollectorEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps

object EventConsumer {

  def props(eventCollectorManager: ActorRef, bootstrapServers: String, eventTopic: String)(implicit actorMaterializer: ActorMaterializer, executionContext: ExecutionContext): Props =
    Props(new EventConsumer(eventCollectorManager, bootstrapServers, eventTopic))
}

/**
 * Consume Cluster events.
 *
 * @param eventCollectorManager Reference to CollectorEvent Collector Manager, to consume events further.
 */
class EventConsumer(eventCollectorManager: ActorRef, bootstrapServers: String, eventTopic: String)(implicit materializer: ActorMaterializer, executionContext: ExecutionContext)
  extends Actor {

  val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(context.system, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withPollInterval(500 millis)
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId("_kafka-event-collector-internal-consumer")

  val killSwitch: UniqueKillSwitch =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(eventTopic))
      .map(record => CollectorEvent.parseFrom(record.value()))
      .mapAsync(1)(event => Future {
        eventCollectorManager ! event
      })
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.ignore)
      .run()

  override def receive: Receive = Actor.emptyBehavior

  override def postStop(): Unit = killSwitch.shutdown()
}
