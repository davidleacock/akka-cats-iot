package service

import actors.NotifierActor.{ NotifierMessage, Notify }
import actors.{ NotifierActor, PersistentDevice }
import actors.PersistentDevice.Command.{
  AlertDevice,
  GetDeviceState,
  InitializeDevice
}
import actors.PersistentDevice.Response
import actors.PersistentDevice.Response.DeviceResponse
import actors.PersistentDevice.State.{ ALERTING, MONITORING }
import akka.Done
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ ExecutionContext, Future }

// TODO do something about the code duplication

// TODO in order to make this more interesting of a project there needs to be another service and then this service
// aggregates over them both. Can use EitherT as well I think
class DefaultIotDeviceService(
  sharding: ClusterSharding,
  notifierActor: ActorRef[NotifierMessage]
)(implicit ec: ExecutionContext,
  implicit val system: ActorSystem[_])
    extends IotDeviceService {

  private val log = LoggerFactory.getLogger(this.getClass)

  private implicit val askTimeout: Timeout = Timeout(5.seconds)

  notifierActor.ask(replyTo => Notify(replyTo))

  override def registerDevice(id: String): Future[Either[String, Done]] = {
    sharding
      .entityRefFor(PersistentDevice.TypeKey, id)
      .ask(replyTo => InitializeDevice(replyTo))
      .map {
        case Done => {
          log.info(s"registerDevice call returned for $id")
          Right(Done)
        }
        case _ => Left(s"registerDevice error returned for $id")
      }
  }

  override def processDeviceEvent(
    id: String,
    message: String
  ): Future[Either[String, Done]] = {
    // TODO THIS IS UGLY AND TERRIBLE

    val future: Future[Either[String, Done.type]] = sharding
      .entityRefFor(PersistentDevice.TypeKey, id)
      .ask(replyTo => AlertDevice(message, replyTo))
      .map {
        case Response.DeviceResponse(device, state) => {
          log.info(s"processDeviceEvent return $id with msg $message")
          val eventualReplyOrDone: Future[Either[String, Done.type]] =
            state match {
              case MONITORING =>
                val eventualStringOrDone: Future[Either[String, Done.type]] =
                  notifierActor.ask(replyTo => Notify(replyTo)).map {
                    case NotifierActor.NotifySuccess => {
                      log.info("Notification Successful")
                      Right(Done)
                    }
                    case NotifierActor.NotifyFailed(reason) => {
                      log.info(s"Notification Failed - reason $reason")
                      Left(reason)
                    }
                  }
                eventualStringOrDone
              case ALERTING =>
                Future(Left("Device is already alerting - will not notify."))
            }

          eventualReplyOrDone
        }
        case _ => {
          log.info(s"processDeviceEvent error return $id with msg $message")
          Future(Left("Error"))
        }
      }
      .flatMap(x => x) // fix this lol

    future
  }

  override def retrieveDevice(id: String): Future[Either[String, String]] = {
    sharding
      .entityRefFor(PersistentDevice.TypeKey, id)
      .ask(replyTo => GetDeviceState(replyTo))
      .map {
        case Response.DeviceResponse(device, state) => {
          log.info(s"processDeviceEvent return $device in $state")
          Right(s"${device.id} ${device.maybeMessage.map(_.toString)} $state")
        }
        case _ => {
          log.info(s"processDeviceEvent return error for $id")
          Left("Error")
        }
      }
  }
}
