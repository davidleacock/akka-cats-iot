package actors

import akka.Done
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  Entity,
  EntityTypeKey
}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }

object PersistentDevice {

  sealed trait Command

  object Command {
    case class InitializeDevice(replyTo: ActorRef[Done]) extends Command

    case class DisableDevice(replyTo: ActorRef[Done]) extends Command

    case class AlertDevice(message: String, replyTo: ActorRef[Response])
        extends Command

    case class StopAlert(replyTo: ActorRef[Done]) extends Command

    case class GetDeviceState(replyTo: ActorRef[Response]) extends Command
  }

  sealed trait Response

  object Response {
    case class DeviceResponse(device: Device, state: String) extends Response
    case class FailureResponse(message: String) extends Response
  }

  sealed trait Event

  object Event {
    case class DeviceInitialized() extends Event

    case class DeviceAlerted(message: String) extends Event

    case class DeviceAlertStopped() extends Event

    case class DeviceDisabled() extends Event
  }

  sealed trait State

  object State {
    case class Inactive(device: Device) extends State

    case class Monitoring(device: Device) extends State

    case class Alerting(device: Device) extends State

    val INACTIVE = "Inactive"
    val MONITORING = "Monitoring"
    val ALERTING = "Alerting"
  }

  case class Device(id: String, maybeMessage: Option[String])
  import Command._
  import Event._
  import Response._
  import State._

  val commandHandler: (State, Command) => Effect[Event, State] =
    (state, command) => {
      state match {
        case Inactive(device) =>
          command match {
            case InitializeDevice(replyTo) =>
              Effect
                .persist(DeviceInitialized())
                .thenReply(replyTo)(_ => Done)
            case GetDeviceState(replyTo) =>
              Effect.reply(replyTo)(DeviceResponse(device, INACTIVE))
            case _ => Effect.none
          }
        case Monitoring(device) =>
          command match {
            case AlertDevice(message, replyTo) => {
              Effect
                .persist(DeviceAlerted(message))
                .thenReply(replyTo)(_ =>
                  DeviceResponse(
                    device.copy(maybeMessage = Some(message)),
                    MONITORING
                  )
                )
            }
            case DisableDevice(replyTo) =>
              Effect
                .persist(DeviceDisabled())
                .thenReply(replyTo)(_ => Done)
            case GetDeviceState(replyTo) =>
              Effect.reply(replyTo)(DeviceResponse(device, MONITORING))
            case _ => Effect.none
          }
        case Alerting(device) =>
          command match {
            case StopAlert(replyTo) =>
              Effect
                .persist(DeviceAlertStopped())
                .thenReply(replyTo)(_ => Done)
            case GetDeviceState(replyTo) =>
              Effect.reply(replyTo)(DeviceResponse(device, ALERTING))
            case AlertDevice(message, replyTo) => {
              Effect
                .persist(DeviceAlerted(message))
                .thenReply(replyTo)(_ => DeviceResponse(device, ALERTING))
            }
            case _ => Effect.none
          }
      }
    }

  val eventHandler: (State, Event) => State =
    (state, event) =>
      state match {
        case inactive @ Inactive(device) =>
          event match {
            case DeviceInitialized() => Monitoring(device)
            case _                   => inactive
          }
        case monitoring @ Monitoring(device) =>
          event match {
            case DeviceAlerted(message) =>
              Alerting(device.copy(maybeMessage = Some(message)))
            case DeviceDisabled() => Inactive(device)
            case _                => monitoring
          }
        case alerting @ Alerting(device) =>
          event match {
            case DeviceAlertStopped() =>
              Monitoring(device.copy(maybeMessage = None))
            case _ => alerting
          }
      }

  val TypeKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("PersistentDevice")

  def initSharding(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(Entity(TypeKey) { entityContext =>
      PersistentDevice(entityContext.entityId)
    })
  }

  def apply(deviceId: String): Behavior[Command] = {
    EventSourcedBehavior[Command, Event, State](
      persistenceId = PersistenceId(EntityTypeKey.toString, deviceId),
      emptyState = Inactive(Device(deviceId, None)),
      commandHandler = commandHandler,
      eventHandler = eventHandler
    )
  }
}
