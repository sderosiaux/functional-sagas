package com.sderosiaux

import cats.effect.IO
import com.sderosiaux.SagaMessage._
import com.sderosiaux.SagaMessageType.StartSaga
import com.sderosiaux.SagaRecoveryType.ForwardRecovery
import org.scalatest.{FlatSpec, Matchers}

class SagaTest extends FlatSpec with Matchers {
  val data = new Data {}
  val sagaId = "my saga"
  val taskId = "my lonely task"

  "Saga" should "work" in {
    val coord = SagaCoordinator.createInMemory().unsafeRunSync()
    val saga = coord.createSaga(sagaId, data).unsafeRunSync()
    saga.id shouldBe sagaId
  }

  "Sagas" should "be recoverable" in {
    val existingLogs = Map(sagaId -> List(startSaga(sagaId, data), startTask(sagaId, taskId, data), endTask(sagaId, taskId, data), endSaga(sagaId)))
    val coord = SagaCoordinator.createInMemory(existingLogs).unsafeRunSync()
    val saga = coord.recoverSaga(sagaId, ForwardRecovery).unsafeRunSync()

    saga.id shouldBe sagaId
    saga.state.completed shouldBe true
    saga.state.aborted shouldBe false
  }

  "Sagas" should "abort" in {
    (for {
      coord <- SagaCoordinator.createInMemory()
      saga <- coord.createSaga(sagaId, data)
      _ = continue(coord, saga)
    } yield ()).unsafeRunSync()

    def continue(coord: SagaCoordinator[IO], saga: Saga[IO]): Unit = {

      saga.startTask(taskId, data)
      saga.abort()
      saga.startCompensatingTask(taskId, data)
      saga.endCompensatingTask(taskId, data)

      Thread.sleep(200)

      val log: SagaLog[IO] = coord.log
      val messages = log.messages(sagaId).unsafeRunSync()
      messages.head shouldBe startSaga(sagaId, data)
      messages(1) shouldBe startTask(sagaId, taskId, data)
      messages(2) shouldBe abortSaga(sagaId)
      messages(3) shouldBe startCompTask(sagaId, taskId, data)
      messages(4) shouldBe endCompTask(sagaId, taskId, data)

      saga.state.isCompTaskCompleted(taskId) shouldBe true
    }
  }
}
