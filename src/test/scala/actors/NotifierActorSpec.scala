package actors

import akka.Done
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import notifier.Notifier
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future

class NotifierActorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  import NotifierActor._
  import NotifierMocks._

  "NotifierActor" must {
    val probe = createTestProbe[NotifierReply]()

    "successfully call notifier and return NotifySuccess result" in {
      val notifierActor = spawn(NotifierActor(mockSuccessNotifier))

      notifierActor ! NotifierActor.Notify(probe.ref)
      val notifierReply = probe.receiveMessage()
      val replyPayload = notifierReply match {
        case NotifySuccess        => NotifySuccess
        case NotifyFailed(reason) => reason
      }

      replyPayload shouldBe NotifySuccess
    }

    "fail to call notifier and return exception message" in {
      val notifierActor = spawn(NotifierActor(mockFailureNotifier))

      notifierActor ! NotifierActor.Notify(probe.ref)
      val notifierReply = probe.receiveMessage()
      val replyPayload = notifierReply match {
        case NotifySuccess        => NotifySuccess
        case NotifyFailed(reason) => reason
      }
      replyPayload shouldBe exceptionMessage
    }
  }
}

object NotifierMocks {
  val exceptionMessage = "problem"

  val mockSuccessNotifier: Notifier[Done] = new Notifier[Done] {
    override def sendNotification: Future[Done] = Future.successful(Done)

    override def getType: String = "dummy"
  }

  val mockFailureNotifier: Notifier[Done] = new Notifier[Done] {
    override def sendNotification: Future[Done] =
      Future.failed(new RuntimeException(exceptionMessage))

    override def getType: String = "bad"
  }
}
