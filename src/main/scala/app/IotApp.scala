package app

import actors.PersistentDevice.Command
import actors.{NotifierActor, PersistentDevice}
import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import http.{IotDeviceServiceWebServer, Routes}
import notifier.HttpNotifier
import service.DefaultIotDeviceService

import scala.concurrent.ExecutionContext.Implicits.global

object IotApp {

  def main(args: Array[String]): Unit =
    ActorSystem[Command](behavior(), "iot-app")

  private def behavior(): Behavior[Command] = {
    Behaviors.setup { context =>
      implicit val system: ActorSystem[_] = context.system
      val sharding = ClusterSharding(system)

      sharding.init(Entity(PersistentDevice.TypeKey) { entityContext =>
        PersistentDevice(entityContext.entityId)
      })

      val httpNotifier = new HttpNotifier("url") // grab from config
      val notifierRef = context.spawn(NotifierActor(httpNotifier), "Notifier")
      val iotDeviceService = new DefaultIotDeviceService(sharding, notifierRef)

      // TODO This will be sharded so needs to be builds from config
      IotDeviceServiceWebServer.start(
        routes = new Routes(iotDeviceService).routes,
        port = 1234, // grab from config
        system
      )

      Behaviors.empty
    }
  }
}
