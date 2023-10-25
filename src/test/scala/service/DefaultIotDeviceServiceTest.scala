//package service
//
//import actors.NotifierActor
//import akka.Done
//import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
//import akka.actor.typed.Behavior
//import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
//import akka.util.Timeout
//import com.typesafe.config.ConfigFactory
//import notifier.Notifier
//import org.scalatest.wordspec.AnyWordSpecLike
//
//import java.util.UUID
//import scala.concurrent.duration.DurationInt
//import scala.concurrent.{ExecutionContext, Future}
//import scala.util.{Failure, Success}
//
//class DefaultIotDeviceServiceTest
//    extends ScalaTestWithActorTestKit(
//      ConfigFactory
//        .parseString(
//          """
//            |akka.actor.allow-java-serialization = on
//            |akka {
//            |  actor {
//            |    provider = "cluster"
//            |  }
//            |  remote.artery {
//            |    canonical {
//            |      hostname = "127.0.0.1"
//            |      port = 2551
//            |    }
//            |  }
//            |
//            |  cluster {
//            |    seed-nodes = [
//            |      "akka://ClusterSystem@127.0.0.1:2551",
//            |      "akka://ClusterSystem@127.0.0.1:2552"]
//            |
//            |    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
//            |  }
//            |}
//            |""".stripMargin)
//    )
//    with AnyWordSpecLike {
//
//  "DefaultIotDeviceService" must {
//
//    implicit val timeout: Timeout = Timeout(5.seconds)
//    implicit val ec: ExecutionContext = system.executionContext
//
//    val notifier = new Notifier[Done] {
//      override def sendNotification: Future[Done] = {
//        system.log.info("Sending notification")
//        Future.successful(Done)
//      }
//
//      override def getType: String = "dummy"
//    }
//
//    val notifierActor: Behavior[NotifierActor.NotifierMessage] = NotifierActor(notifier)
//
//    val service = new DefaultIotDeviceService(system, notifierActor)
//
//    "register a device and return Done" in {
//      val name = "device_name"
//      val id = UUID.randomUUID().toString
//
//      val serviceReply: Future[Either[String, Done]] = service.registerDevice(id, name)
//      serviceReply onComplete {
//        case Success(value) => value match {
//          case Left(badNewsString) => system.log.info("Left badnewsstring")
//          case Right(goodNewsDone) => system.log.info("Right goodnewsdone")
//        }
//        case Failure(exception) => system.log.info("future failure")
//      }
//
//      Thread.sleep(5000)
//    }
//  }
//}
