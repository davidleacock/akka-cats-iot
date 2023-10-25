package playground.clustering

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.typesafe.config.ConfigFactory

import java.util.{Date, UUID}
import scala.util.Random

// Playground to experiment with ClusterSharding concepts

case class OysterCard(id: String, amount: Double)
case class EntryAttempt(oysterCard: OysterCard, date: Date)
case object EntryAccepted
case class EntryRejected(reason: String)

object Turnstile {
  def props(validator: ActorRef): Props = Props(new Turnstile(validator))
}

class Turnstile(validator: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {
    case o: OysterCard         => validator ! EntryAttempt(o, new Date)
    case EntryAccepted         => log.info("GREEN - go")
    case EntryRejected(reason) => log.info(s"RED $reason")
  }
}

class OysterCardValidator extends Actor with ActorLogging {
  override def receive: Receive = {
    case EntryAttempt(card @ OysterCard(id, amount), _) => {
      log.info(s"Validating $card")
      if (amount > 2.5) sender() ! EntryAccepted
      else sender() ! EntryRejected(s"$id not enough funds")
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    log.info("***** Validator Starting *****")
  }
}

// Shard settings
object TurnstileSettings {
  val numberOfShards = 10 // use 10x number of nodes
  val numberOfEntities = 100 // 10x number of shards

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case attempt @ EntryAttempt(OysterCard(cardId, _), _) =>
      val entityId = cardId.hashCode.abs % numberOfEntities
      (entityId.toString, attempt)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case EntryAttempt(OysterCard(cardId, _), _) =>
      val shardId = cardId.hashCode.abs % numberOfShards
      shardId.toString
  }
}

// cluster nodes
class TubeStation(port: Int, numberOfTurnstiles: Int) extends App {

  val config = ConfigFactory
    .parseString(s"""
                    |akka.remote.artery.canonical.port = $port
                    |""".stripMargin)
    .withFallback(
      ConfigFactory.load("playground/clustering/clusterSharding.conf")
    )

  val system = ActorSystem("ClusterSystem", config)
  val validatorShardRegionReg: ActorRef = ClusterSharding(system).start(
    typeName = "OysterCardValidator",
    entityProps = Props[OysterCardValidator],
    settings = ClusterShardingSettings(system),
    extractEntityId = TurnstileSettings.extractEntityId,
    extractShardId = TurnstileSettings.extractShardId
  )

  val turnstiles = (1 to numberOfTurnstiles).map {
    _ => system.actorOf(Turnstile.props(validatorShardRegionReg))
  }

  Thread.sleep(10000)
  for (_ <- 1 to 1000) {
    val randomTurnstileIndex = Random.nextInt(numberOfTurnstiles)
    val randomTurnstile = turnstiles(randomTurnstileIndex)

    randomTurnstile ! OysterCard(UUID.randomUUID().toString, Random.nextDouble() * 10)
    Thread.sleep(200)
  }
}

object Station1 extends TubeStation(2551, 10)
object Station2 extends TubeStation(2552, 5)
object Station3 extends TubeStation(2553, 15)