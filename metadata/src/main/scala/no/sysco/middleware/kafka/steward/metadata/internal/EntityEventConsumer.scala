package no.sysco.middleware.kafka.steward.metadata.internal

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import no.sysco.middleware.kafka.steward.proto.metadata.MetadataEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

object EntityEventConsumer {

  def props(entityManager: ActorRef,
            bootstrapServers: String,
            eventTopic: String)
           (implicit
            actorMaterializer: ActorMaterializer,
            executionContext: ExecutionContext): Props =
    Props(new EntityEventConsumer(entityManager, bootstrapServers, eventTopic))
}

class EntityEventConsumer(entityManager: ActorRef,
                          bootstrapServers: String,
                          eventTopic: String)
                         (implicit actorMaterializer: ActorMaterializer,
                          executionContext: ExecutionContext) extends Actor with ActorLogging {

  val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(context.system, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withPollInterval(500 millis)
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId("_kafka-steward-metadata-internal-consumer")

  val killSwitch: UniqueKillSwitch =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(eventTopic))
      .map(record => MetadataEvent.parseFrom(record.value()))
      .mapAsync(1)(event => Future {
        entityManager ! event
      })
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.ignore)
      .run()

  override def receive: Receive = Actor.emptyBehavior

  override def postStop(): Unit = killSwitch.shutdown()
}
