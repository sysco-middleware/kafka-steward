package no.sysco.middleware.kafka.event.collector.cluster.internal

import akka.actor.{ Actor, ActorRef }
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ ConsumerSettings, Subscriptions }
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.{ ActorMaterializer, KillSwitches, UniqueKillSwitch }
import com.typesafe.config.Config
import no.sysco.middleware.kafka.event.proto.collector.ClusterEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps

/**
 * Consume Cluster events.
 *
 * @param clusterManager Reference to Cluster Manager, to consume events further.
 */
class ClusterEventConsumer(clusterManager: ActorRef, bootstrapServers: String, clusterEventTopic: String)(implicit materializer: ActorMaterializer, executionContext: ExecutionContext)
  extends Actor {

  val config: Config = context.system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withPollInterval(100 millis)
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId("_kafka-event-collector-internal-consumer")

  val killSwitch: UniqueKillSwitch =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(clusterEventTopic))
      .map(record => ClusterEvent.parseFrom(record.value()))
      .mapAsync(1)(clusterEvent => Future {
        clusterManager ! clusterEvent
      })
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.ignore)
      .run()

  override def receive: Receive = Actor.emptyBehavior

  override def postStop(): Unit = killSwitch.shutdown()
}
