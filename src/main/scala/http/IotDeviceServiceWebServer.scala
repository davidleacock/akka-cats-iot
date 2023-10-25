package http

import akka.actor
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route

import scala.util.{ Failure, Success }

object IotDeviceServiceWebServer {

  def start(routes: Route, port: Int, system: ActorSystem[_]): Unit = {
    import akka.actor.typed.scaladsl.adapter._
    import system.executionContext
    implicit val classic: actor.ActorSystem = system.toClassic

    Http()
      .newServerAt("localhost", port)
      .bind(routes)
      .onComplete {
        case Success(binding) =>
          system
            .log
            .info(
              s"\u001B[34m **** IotDeviceService WebServer online at " +
                s"${binding.localAddress.getHostString}:${binding.localAddress.getPort} **** \u001B[0m"
            )
        case Failure(exception) =>
          system.log.error(s"Failed to bind HTTP server $exception")
          system.terminate()
      }
  }
}
