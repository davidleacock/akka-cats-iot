package actors

import actors.NotifierActor.NotifierMessage
import akka.Done
import akka.actor.typed.scaladsl.{ AbstractBehavior, ActorContext, Behaviors }
import akka.actor.typed.{ ActorRef, Behavior }
import notifier.Notifier

import scala.util.{ Failure, Success }

object NotifierActor {
  def apply(notifier: Notifier[Done]): Behavior[NotifierMessage] =
    Behaviors.setup(context => new NotifierActor(context, notifier))
  sealed trait NotifierMessage

  case class Notify(replyTo: ActorRef[NotifierReply]) extends NotifierMessage

  sealed trait NotifierReply

  object NotifySuccess extends NotifierReply
  case class NotifyFailed(reason: String) extends NotifierReply
}

class NotifierActor(
  context: ActorContext[NotifierMessage],
  notifier: Notifier[Done])
    extends AbstractBehavior[NotifierMessage](context) {
  selfBehavior =>

  import NotifierActor._

  override def onMessage(msg: NotifierMessage): Behavior[NotifierMessage] = {
    case class WrappedNotifierReply(
      notifierResponse: NotifierReply,
      replyTo: ActorRef[NotifierReply])
        extends NotifierMessage

    msg match {
      case Notify(replyTo) => {
        context.pipeToSelf(notifier.sendNotification) {
          case Success(_) =>
            WrappedNotifierReply(NotifySuccess, replyTo)
          case Failure(exception) =>
            WrappedNotifierReply(NotifyFailed(exception.getMessage), replyTo)
        }
      }
      case WrappedNotifierReply(notifierResponse, replyTo) =>
        replyTo ! notifierResponse
      case _ => context.log.error("Unknown message.")
    }

    selfBehavior
  }
}
