package notifier

import scala.concurrent.Future

trait Notifier[T] {
  def sendNotification: Future[T]
  def getType: String
}
