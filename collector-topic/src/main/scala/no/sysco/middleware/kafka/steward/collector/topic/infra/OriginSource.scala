package no.sysco.middleware.kafka.steward.collector.topic.infra

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import no.sysco.middleware.kafka.steward.collector.proto.topic.{ TopicKey, TopicValue }
import no.sysco.middleware.kafka.steward.collector.topic.core.model._
import no.sysco.middleware.kafka.steward.collector.topic.infra.OriginSource.Poll
import org.apache.kafka.clients.consumer.Consumer

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object OriginSource {

  def props(consumer: Consumer[Array[Byte], Array[Byte]], topic: String, topicsRef: ActorRef): Props =
    Props(new OriginSource(consumer, topic, topicsRef))

  case class Poll()

}

class OriginSource(consumer: Consumer[Array[Byte], Array[Byte]], topic: String, topicsRef: ActorRef) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    consumer.subscribe(List(topic).asJava)
    self ! Poll()
  }

  override def postStop(): Unit = consumer.close(3, TimeUnit.SECONDS)

  override def receive: Receive = {
    case Poll() => handlePoll()
  }

  @tailrec
  private def handlePoll(): Unit = {
    val records = consumer.poll(Duration.ofSeconds(1).toMillis)
    records.forEach { record =>
      log.info("Record consumer: {}-{}", record.topic(), record.offset())
      try {
        val topicKey = TopicKey.parseFrom(record.key())
        val topicValue = TopicValue.parseFrom(record.value())
        topicValue match {
          case TopicValue(event) if event.isTopicCreated => // do nothing
          case TopicValue(event) if event.isTopicUpdated =>
            val topicUpdated = event.topicUpdated.get
            topicsRef !
              TopicUpdated(
                ClusterId(topicKey.clusterId),
                Topic(
                  topicKey.name,
                  topicUpdated.partitions.map {
                    tp => Partition(tp.id, tp.replicas.map { r => Replica(BrokerId(r.id)) }.toSet)
                  }.toSet,
                  Config(topicUpdated.config.get.entries),
                  topicUpdated.internal))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    handlePoll()
  }

}
