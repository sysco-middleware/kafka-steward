package no.sysco.middleware.kafka.steward.collector.internal

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ ConsumerSettings, Subscriptions }
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.{ ActorMaterializer, KillSwitches, UniqueKillSwitch }
import no.sysco.middleware.kafka.steward.proto.collector.CollectorEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps

object CollectorEventConsumer {

  def props(
    eventCollectorManager: ActorRef,
    bootstrapServers: String,
    eventTopic: String)(implicit
    actorMaterializer: ActorMaterializer,
    executionContext: ExecutionContext): Props =
    Props(new CollectorEventConsumer(eventCollectorManager, bootstrapServers, eventTopic))
}

/**
 * Consume Collector events.
 *
 * @param collectorManager Reference to Collector Manager, which consume this events.
 */
class CollectorEventConsumer(
  collectorManager: ActorRef,
  bootstrapServers: String,
  eventTopic: String)(implicit
  materializer: ActorMaterializer,
  executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(context.system, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withPollInterval(500 millis)
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId("_kafka-steward-collector-internal-consumer")

  val killSwitch: UniqueKillSwitch =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(eventTopic))
      .map(record => CollectorEvent.parseFrom(record.value()))
      .mapAsync(1)(event => Future {
        collectorManager ! event
      })
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.ignore)
      .run()

  override def receive: Receive = Actor.emptyBehavior

  override def postStop(): Unit = killSwitch.shutdown()
}
