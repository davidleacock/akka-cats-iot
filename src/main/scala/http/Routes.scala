package http

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import service.IotDeviceService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// todo fix global ec import
class Routes(iotDeviceService: IotDeviceService) {

  final case class ServiceResponse(message: String)
  final case class ProcessDeviceEvent(deviceId: String, message: String)

  private def registerDevice(deviceId: String): Future[ServiceResponse] = {
    iotDeviceService
      .registerDevice(deviceId)
      .map {
        case Left(error) => ServiceResponse(s"Register Device Error - $error")
        case _ => ServiceResponse(s"Register Device Successful - $deviceId")
      }
  }

  private def processDeviceEvent(
    deviceId: String,
    message: String
  ): Future[ServiceResponse] = {
    iotDeviceService
      .processDeviceEvent(deviceId, message)
      .map {
        case Left(error) =>
          ServiceResponse(s"Process Device Event Error - $error")
        case _ =>
          ServiceResponse(s"Process Device Successful - $deviceId $message")
      }
  }

  private def retrieveDevice(deviceId: String): Future[ServiceResponse] = {
    iotDeviceService
      .retrieveDevice(deviceId)
      .map {
        case Left(error) => ServiceResponse(s"Retrieve Device Error - $error")
        case Right(value) =>
          ServiceResponse(s"Retrieve Device Successful - $value")
      }
  }

  def extractGitlabEvent: HttpHeader => Option[String] = {
    header: HttpHeader => {
      println(header)
      Some("done")
    }
  }


  val routes: Route =
    optionalHeaderValue(extractGitlabEvent) { header =>
      println(s"header: $header")
      pathPrefix("device") {
        path(Segment) { deviceId =>
          put {
            onSuccess(registerDevice(deviceId)) { case ServiceResponse(message) =>
              complete(message)
            } ~
              get {
                onSuccess(retrieveDevice(deviceId)) {
                  case ServiceResponse(message) =>
                    complete(message)
                }
              } ~
              put {
                entity(as[ProcessDeviceEvent]) { request =>
                  onSuccess(
                    processDeviceEvent(request.deviceId, request.message)
                  ) { response =>
                    complete(s"${response.message}")
                  }
                }
              }
          }
        }
      }
    }



}
