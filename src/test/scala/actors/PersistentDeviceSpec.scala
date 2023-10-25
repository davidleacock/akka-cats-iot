package actors

import actors.PersistentDevice.Command._
import actors.PersistentDevice.Response._
import actors.PersistentDevice.State._
import actors.PersistentDevice._
import akka.Done
import akka.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class PersistentDeviceSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory
        .parseString("akka.actor.allow-java-serialization = on")
        .withFallback(EventSourcedBehaviorTestKit.config)
    )
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with LogCapturing {

  private val id: String = UUID.randomUUID().toString
  private val probeWaitDuration = FiniteDuration(250, TimeUnit.MILLISECONDS)

  private val eventSourcedTestKit
    : EventSourcedBehaviorTestKit[Command, Event, State] =
    EventSourcedBehaviorTestKit[Command, Event, State](
      system,
      PersistentDevice(id)
    )

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    eventSourcedTestKit.clear()
  }

  "A Device" must {
    val device = Device(id, None)
    val alertMessage = "alert_message"
    val alertedDevice = Device(id, Some(alertMessage))

    "be created in inactive state" in {
      val result = eventSourcedTestKit.runCommand[Response](replyTo =>
        GetDeviceState(replyTo)
      )
      result.reply shouldBe DeviceResponse(device, INACTIVE)
      result.stateOfType[Inactive].device shouldBe device
    }

    "not become in an alerting state without initialization" in {
      val testProbe = testKit.createTestProbe[Response]("test_probe")
      val result =
        eventSourcedTestKit.runCommand(AlertDevice(alertMessage, testProbe.ref))

      testProbe.expectNoMessage(probeWaitDuration)
      result.hasNoEvents shouldBe true
    }

    "start monitoring once initialized" in {
      val result = eventSourcedTestKit.runCommand[Done](replyTo =>
        InitializeDevice(replyTo)
      )
      result.reply shouldBe Done
      result.stateOfType[Monitoring].device shouldBe device
    }

    "become alerting when device is alerted after being initialized" in {
      eventSourcedTestKit.runCommand[Done](replyTo => InitializeDevice(replyTo))

      val result = eventSourcedTestKit.runCommand[Response](replyTo =>
        AlertDevice(alertMessage, replyTo)
      )
      result.reply shouldBe DeviceResponse(alertedDevice, MONITORING)
      result.stateOfType[Alerting].device shouldBe alertedDevice
    }

    "reply with alert message when queried when device is alerted" in {
      eventSourcedTestKit.runCommand[Done](replyTo => InitializeDevice(replyTo))
      eventSourcedTestKit.runCommand[Response](replyTo =>
        AlertDevice(alertMessage, replyTo)
      )
      val result = eventSourcedTestKit.runCommand[Response](replyTo =>
        GetDeviceState(replyTo)
      )

      result.reply shouldBe DeviceResponse(alertedDevice, ALERTING)
      result.stateOfType[Alerting].device shouldBe alertedDevice
    }

    "stop alerting and go back to monitoring" in {
      eventSourcedTestKit.runCommand[Done](replyTo => InitializeDevice(replyTo))
      eventSourcedTestKit.runCommand[Response](replyTo =>
        AlertDevice(alertMessage, replyTo)
      )

      val result =
        eventSourcedTestKit.runCommand[Done](replyTo => StopAlert(replyTo))
      result.reply shouldBe Done
      result.stateOfType[Monitoring].device shouldBe device
    }

    "reply with no message when queried when device is back to monitor after alerted" in {
      eventSourcedTestKit.runCommand[Done](replyTo => InitializeDevice(replyTo))
      eventSourcedTestKit.runCommand[Response](replyTo => AlertDevice(alertMessage, replyTo))
      eventSourcedTestKit.runCommand[Done](replyTo => StopAlert(replyTo))

      val result = eventSourcedTestKit.runCommand[Response](replyTo =>
        GetDeviceState(replyTo)
      )

      result.reply shouldBe DeviceResponse(device, MONITORING)
      result.stateOfType[Monitoring].device shouldBe device
    }

    "not become disabled without an alert being stopping first" in {
      eventSourcedTestKit.runCommand[Done](replyTo => InitializeDevice(replyTo))
      eventSourcedTestKit.runCommand[Response](replyTo => AlertDevice(alertMessage, replyTo))

      val testProbe = testKit.createTestProbe[Done]("test_probe")
      val result = eventSourcedTestKit.runCommand(DisableDevice(testProbe.ref))

      testProbe.expectNoMessage(probeWaitDuration)
      result.hasNoEvents shouldBe true
    }
  }
}
