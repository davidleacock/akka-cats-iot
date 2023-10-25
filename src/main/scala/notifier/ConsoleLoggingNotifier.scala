package notifier

import scala.concurrent.Future

object ConsoleLoggingNotifier extends Notifier[String] {
  override def getType: String = "ConsoleLogging"

  override def sendNotification: Future[String] = {
    println("Sending notification... sent")
    Future.successful("Done")
  }
}
