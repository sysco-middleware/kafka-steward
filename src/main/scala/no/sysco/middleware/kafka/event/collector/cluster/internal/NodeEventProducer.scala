package no.sysco.middleware.kafka.event.collector.cluster.internal

import akka.actor.Actor
import no.sysco.middleware.kafka.event.proto.collector.NodeEvent
import org.apache.kafka.clients.producer.{ Producer, ProducerRecord }

/**
 * Publish Cluster events.
 */
class NodeEventProducer(nodeEventTopic: String, producer: Producer[Int, Array[Byte]]) extends Actor {

  def handleNodeEvent(nodeEvent: NodeEvent): Unit = {
    val byteArray = nodeEvent.toByteArray
    producer.send(new ProducerRecord(nodeEventTopic, nodeEvent.id, byteArray)).get()
  }

  override def receive: Receive = {
    case nodeEvent: NodeEvent => handleNodeEvent(nodeEvent)
  }

  override def postStop(): Unit = producer.close()
}
