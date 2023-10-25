package service
import actors.NotifierActor.{NotifierMessage, NotifySuccess}
import actors.PersistentDevice.Command.{AlertDevice, GetDeviceState, InitializeDevice}
import actors.PersistentDevice.Response.DeviceResponse
import actors.PersistentDevice.State.MONITORING
import actors.PersistentDevice.{Command, Device, Response}
import actors.{NotifierActor, PersistentDevice}
import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityRef}
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.util.Timeout
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.mockito.captor.{ArgCaptor, Captor}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterEach, EitherValues}

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DefaultIotDeviceServiceSpec
    extends AnyWordSpecLike
    with Matchers
    with IdiomaticMockito
    with BeforeAndAfterEach
    with ScalaFutures
    with EitherValues {

  type ActorRefF[T] = ActorRef[T] => Command

  private implicit val mockSystem: ActorSystem[_] = ActorTestKit.apply().system

  private val mockClusterSharding = mock[ClusterSharding]
  private val mockReplyToDone = mock[ActorRef[Done]]
  private val mockReplyToResponse = mock[ActorRef[Response]]
  private val mockEntityRef = mock[EntityRef[Command]]

  private val mockNotifier: Behavior[NotifierMessage] = Behaviors.setup[NotifierMessage] { context =>
    Behaviors.receiveMessage {
      case NotifierActor.Notify(replyTo) => {
        context.system.log.info("Notifier Actor notifying...")
        replyTo ! NotifySuccess
      }
        Behaviors.same
    }
  }

  private val thing: ActorRef[NotifierMessage] = mockSystem.systemActorOf(mockNotifier, "thing")

  private val id = UUID.randomUUID().toString

  private val service =
    new DefaultIotDeviceService(mockClusterSharding, thing)

  override def beforeEach(): Unit = reset(mockClusterSharding, mockEntityRef)

  "registerDevice" should {
    "register the device" in {
      val command = InitializeDevice(mockReplyToDone)

      when(mockClusterSharding.entityRefFor(PersistentDevice.TypeKey, id))
        .thenReturn(mockEntityRef)

      when(mockEntityRef.ask(any[ActorRefF[Done]])(any[Timeout]))
        .thenReturn(Future(Done))

      val result: Future[Either[String, Done]] = service.registerDevice(id)

      result.futureValue shouldBe Right(Done)

      val argCaptor: Captor[ActorRefF[Done]] = ArgCaptor[ActorRefF[Done]]

      verify(mockEntityRef).ask(argCaptor.capture)(any[Timeout])

      val captured: ActorRefF[Done] = argCaptor.value

      captured(mockReplyToDone) shouldBe command
    }

    "thing" in {

      val request = Post().withEntity()

    }
  }

  "processDeviceEvent" should {
    "process a device alert and it's message" in {
      val alertMessage = "alert_message"
      val command = AlertDevice(alertMessage, mockReplyToResponse)
      val deviceResponse = DeviceResponse(Device(id, None), MONITORING)

      when(mockClusterSharding.entityRefFor(PersistentDevice.TypeKey, id))
        .thenReturn(mockEntityRef)

      when(mockEntityRef.ask(any[ActorRefF[Response]])(any[Timeout]))
        .thenReturn(Future(deviceResponse))

      val result = service.processDeviceEvent(id, alertMessage)

      result.futureValue shouldBe Right(Done)

      val argCaptor: Captor[ActorRefF[Response]] =
        ArgCaptor[ActorRefF[Response]]

      verify(mockEntityRef).ask(argCaptor.capture)(any[Timeout])

      val captured: ActorRefF[Response] = argCaptor.value

      captured(mockReplyToResponse) shouldBe command
    }
  }

  "retrieveDevice" should {
    "retrieve a device" in {
      val command = GetDeviceState(mockReplyToResponse)
      val deviceResponse = DeviceResponse(Device(id, None), MONITORING)

      when(mockClusterSharding.entityRefFor(PersistentDevice.TypeKey, id))
        .thenReturn(mockEntityRef)

      when(mockEntityRef.ask(any[ActorRefF[Response]])(any[Timeout]))
        .thenReturn(Future(deviceResponse))

      val result = service.retrieveDevice(id)

      result.futureValue shouldBe Right(s"$id None $MONITORING")

      val argCaptor: Captor[ActorRefF[Response]] =
        ArgCaptor[ActorRefF[Response]]

      verify(mockEntityRef).ask(argCaptor.capture)(any[Timeout])

      val captured: ActorRefF[Response] = argCaptor.value

      captured(mockReplyToResponse) shouldBe command
    }
  }
}
