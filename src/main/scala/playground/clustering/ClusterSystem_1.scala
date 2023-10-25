package playground.clustering

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  Entity,
  EntityRef,
  EntityTypeKey
}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import playground.clustering.Root.{ GetValue, RootCommand, rootBehavior }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Success }

object Root {
  private val logger = LoggerFactory.getLogger(this.getClass)

  trait RootCommand
  case class GetValue(ref: EntityRef[Counter.Command]) extends RootCommand
  case class AdaptedResponse(message: String) extends RootCommand

  val rootBehavior: Behavior[RootCommand] =
    Behaviors.setup[RootCommand] { executionContext =>
      implicit val timeout: Timeout = 3.seconds
      Behaviors.receiveMessage[RootCommand] {
        case GetValue(ref) =>
          executionContext.ask(ref, Counter.GetValue.apply) {
            case Failure(_) =>
              AdaptedResponse("problem")
            case Success(value) =>
              AdaptedResponse(value.toString)
          }
          Behaviors.same
        case AdaptedResponse(message) => {
          logger.info(s"Got response $message")
          Behaviors.same
        }
      }
    }
}

object ServiceA extends ClusterSystem_1(25251)

object ServiceB extends ClusterSystem_2(25254)

class ClusterSystem_1(port: Int) extends App {
  val config = ConfigFactory
    .parseString(s"""
                    |akka.remote.artery.canonical.port = $port
                    |akka.actor.allow-java-serialization = on
                    |akka.actor.warn-about-java-serializer-usage = off
                    |""".stripMargin)
    .withFallback(
      ConfigFactory.load("playground/clustering/clusterSharding.conf")
    )

  private val logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem[RootCommand] =
    ActorSystem(rootBehavior, "ClusterSystem", config)
  val sharding: ClusterSharding = ClusterSharding(system)

  sharding.init(Entity(Counter.TypeKey) { entityContext =>
    Counter(entityContext.entityId, 0)
  })

  private val counter1Ref: EntityRef[Counter.Command] =
    sharding.entityRefFor(Counter.TypeKey, "counter-1")
  private val counter2Ref: EntityRef[Counter.Command] =
    sharding.entityRefFor(Counter.TypeKey, "counter-2")

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val ec: ExecutionContext = system.executionContext

  Thread.sleep(5000)

  logger.info("*** System 1 sending increments ***")

  counter1Ref ! Counter.Increment
  counter2Ref ! Counter.Increment
  counter2Ref ! Counter.Increment
}

class ClusterSystem_2(port: Int) extends App {
  val config = ConfigFactory
    .parseString(s"""
                    |akka.remote.artery.canonical.port = $port
                    |akka.actor.allow-java-serialization = on
                    |akka.actor.warn-about-java-serializer-usage = off
                    |""".stripMargin)
    .withFallback(
      ConfigFactory.load("playground/clustering/clusterSharding.conf")
    )

  private val logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem[RootCommand] =
    ActorSystem(rootBehavior, "ClusterSystem", config)
  val sharding: ClusterSharding = ClusterSharding(system)

  sharding.init(Entity(Counter.TypeKey) { entityContext =>
    Counter(entityContext.entityId, 0)
  })

  private val counter1Ref: EntityRef[Counter.Command] =
    sharding.entityRefFor(Counter.TypeKey, "counter-1")
  private val counter2Ref: EntityRef[Counter.Command] =
    sharding.entityRefFor(Counter.TypeKey, "counter-2")

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val ec: ExecutionContext = system.executionContext

  Thread.sleep(10000)

  logger.info("*** System 2 asking increments ***")

  system.ask[RootCommand](_ => GetValue(counter1Ref))
  system.ask[RootCommand](_ => GetValue(counter2Ref))

}

object Counter {
  private val logger = LoggerFactory.getLogger(this.getClass)

  sealed trait Command
  case object Increment extends Command
  final case class GetValue(replyTo: ActorRef[Int]) extends Command

  def apply(entityId: String, seed: Int): Behavior[Command] = {
    logger.info(s"Counter created with entityId: $entityId")
    def updated(value: Int): Behavior[Command] = {
      Behaviors.receiveMessage[Command] {

        case Increment => {
          val newValue = value + 1
          logger.info(s"Setting $entityId from $value ->  $newValue")
          updated(newValue)
        }
        case GetValue(replyTo) =>
          (replyTo ! value)
          Behaviors.same
      }
    }

    updated(0)
  }

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("Counter")
}
