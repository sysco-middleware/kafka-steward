package no.sysco.middleware.kafka.steward.collector.topic.http

import akka.actor.ActorRef
import akka.http.scaladsl.server.{ Directives, Route }
import akka.pattern.ask
import akka.util.Timeout
import no.sysco.middleware.kafka.steward.collector.topic.TopicCollector.GetTopics
import no.sysco.middleware.kafka.steward.collector.topic.core.model.Topic

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class HttpTopicCollectorService(topicCollectorRef: ActorRef)(implicit executionContext: ExecutionContext)
  extends Directives with TopicCollectorJsonSupport {

  implicit val timeout: Timeout = 5 seconds

  val route: Route =
    path("topics") {
      get {
        val eventualStringToTopic = (topicCollectorRef ? GetTopics()).mapTo[Map[String, Topic]]
        complete(eventualStringToTopic.map(s => s.keys))
      }
    }
}
