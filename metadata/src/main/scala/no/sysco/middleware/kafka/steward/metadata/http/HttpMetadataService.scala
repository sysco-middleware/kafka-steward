package no.sysco.middleware.kafka.steward.metadata.http

import akka.actor.ActorRef
import akka.http.scaladsl.server.{ Directives, Route }

class HttpMetadataService(metadataManager: ActorRef) extends Directives {
  val route: Route = path("entities") {
    path("topics") {
      get {
        complete("")
      }
    } ~
      path("brokers") {
        get {
          complete("")
        }
      }
    // ops by id
  }

}
