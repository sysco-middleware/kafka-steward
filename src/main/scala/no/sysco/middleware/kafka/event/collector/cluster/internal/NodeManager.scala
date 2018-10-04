package no.sysco.middleware.kafka.event.collector.cluster.internal

import akka.actor.{ Actor, Props }
import no.sysco.middleware.kafka.event.collector.model.Node

object NodeManager {
  def props(): Props = Props(new NodeManager())
}

class NodeManager extends Actor {

  var nodes: List[Node] = List()

  override def receive: Receive = ???
}
