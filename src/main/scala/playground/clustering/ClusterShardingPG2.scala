package playground.clustering

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef, EntityTypeKey}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object EchoActor {
  sealed trait Command
  case class Echo(word: String)(val replyTo: ActorRef[Echoing]) extends Command
  case class Echoing(word: String, numberOfTimes: Int) extends Command
  case class Echoed(word: String)

  case class EchoedWords(words: Set[String]) {
    def add(word: String): EchoedWords = copy(words = words + word)

    def totalEchos: Int = words.size
  }

  private val commandHandler
    : (EchoedWords, Command) => Effect[Echoed, EchoedWords] = { (_, command) =>
    command match {
      case echo @ Echo(word) =>
        Effect
          .persist(Echoed(word))
          .thenRun(state => echo.replyTo ! Echoing(echo.word, state.totalEchos))
    }
  }

  private val eventHandler: (EchoedWords, Echoed) => EchoedWords = {
    (state, event) => state.add(event.word)
  }

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("EchoActor")

  def apply(
    entityId: String,
    persistenceId: PersistenceId
  ): Behavior[Command] = {
    Behaviors.setup { context =>
      context.log.info(s"Starting EchoActor $entityId")
      EventSourcedBehavior(
        persistenceId,
        emptyState = EchoedWords(Set.empty),
        commandHandler,
        eventHandler
      )
    }
  }
}

class Service1(port: Int) extends App {
  val config = ConfigFactory
    .parseString(s"""
                    |akka.remote.artery.canonical.port = $port
                    |akka.actor.allow-java-serialization = on
                    |akka.actor.warn-about-java-serializer-usage = off
                    |akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
                    |akka.persistence.snapshot-store.plugin = "akka.persistence.journal.inmem"
                    |""".stripMargin)
    .withFallback(
      ConfigFactory.load("playground/clustering/clusterSharding.conf")
    )

  private val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem[_] =
    ActorSystem[Nothing](Behaviors.empty, "ClusterSystem", config)
  implicit val ec: ExecutionContextExecutor = system.executionContext
  implicit val timeout: Timeout = Timeout(5.seconds)

  val sharding = ClusterSharding(system)

  sharding.init(Entity(typeKey = EchoActor.TypeKey) { entityContext =>
    EchoActor(
      entityContext.entityId,
      PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
    )
  })

  private val entityRef: EntityRef[EchoActor.Command] =
    sharding.entityRefFor(EchoActor.TypeKey, "echo-actor-1")

//  Thread.sleep(15000)
//
//  private val futureEcho: Future[EchoActor.Echoing] =
//    entityRef ? EchoActor.Echo(UUID.randomUUID().toString)
//  futureEcho.onComplete {
//    case Failure(_) => logger.error("Failed!")
//    case Success(value) =>
//      logger.info(
//        s"Echo Success ${value.word} - total echos so far ${value.numberOfTimes}"
//      )
//  }
}

class Service2(port: Int) extends App {
  val config = ConfigFactory
    .parseString(s"""
                    |akka.remote.artery.canonical.port = $port
                    |akka.actor.allow-java-serialization = on
                    |akka.actor.warn-about-java-serializer-usage = off
                    |akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
                    |akka.persistence.snapshot-store.plugin = "akka.persistence.journal.inmem"
                    |""".stripMargin)
    .withFallback(
      ConfigFactory.load("playground/clustering/clusterSharding.conf")
    )

  private val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem[_] =
    ActorSystem[Nothing](Behaviors.empty, "ClusterSystem", config)
  implicit val ec: ExecutionContextExecutor = system.executionContext
  implicit val timeout: Timeout = Timeout(5.seconds)

  val sharding = ClusterSharding(system)

  sharding.init(Entity(typeKey = EchoActor.TypeKey) { entityContext =>
    EchoActor(
      entityContext.entityId,
      PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
    )
  })
//
//  private val entityRef: EntityRef[EchoActor.Command] =
//    sharding.entityRefFor(EchoActor.TypeKey, "echo-actor-1")
//
//  Thread.sleep(5000)
//
//  private val futureEcho: Future[EchoActor.Echoing] =
//    entityRef ? EchoActor.Echo(UUID.randomUUID().toString)
//  futureEcho.onComplete {
//    case Failure(_) => logger.error("Failed!")
//    case Success(value) =>
//      logger.info(
//        s"Echo Success ${value.word} - total echos so far ${value.numberOfTimes}"
//      )
//  }
}

object EchoService extends Service1(25251)
object EchoService2 extends Service2(25254)