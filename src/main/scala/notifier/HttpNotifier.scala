package notifier

import akka.Done
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}

class HttpNotifier(
  url: String
)(implicit val system: ActorSystem[_],
  val ec: ExecutionContext)
    extends Notifier[Done] {

  val logger: Logger = system.log

  override def sendNotification: Future[Done] = {
    logger.info(s"Sending notification to $url")
    Http()
      .singleRequest(HttpRequest(uri = s"$url"))
      .map { resp =>
        resp.discardEntityBytes()
        resp.status
      }
      .flatMap {
        case StatusCodes.OK => {
          logger.info("Got back ok")
          Future.successful(Done)
        }
        case StatusCodes.BadRequest => {
          logger.info("Got back bad request")
          Future.successful(Done)
        }
        case _ => {
          logger.info("NO IDEA")
          Future.failed(new RuntimeException("not sure but its bad"))
        }
      }
      .recoverWith { ex =>
        logger.info("WTF")
        Future.failed(new RuntimeException("bad stuff"))
      }
  }

  override def getType: String = "HttpNotifier"
}
