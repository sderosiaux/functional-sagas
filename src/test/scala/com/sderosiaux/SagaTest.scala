package com.sderosiaux

import cats.effect.IO
import com.sderosiaux.SagaMessage._
import com.sderosiaux.SagaMessageType.StartSaga
import com.sderosiaux.SagaRecoveryType.ForwardRecovery
import org.scalatest.{FlatSpec, Matchers}

class SagaTest extends FlatSpec with Matchers {
    val data = new Data {
      override def toString: String = "data" }
    val sagaId = "my saga"
    val taskId = "my lonely task"

    "Saga" should "work" in {
        val saga = (for {
            coord <- SagaCoordinator.createInMemory()
            saga <- coord.createSaga(sagaId, data)
        } yield saga).unsafeRunSync()
        saga._1.id shouldBe sagaId
    }

    "Sagas" should "be recoverable" in {
        val existingLogs = Map(sagaId -> List(startSaga(sagaId, data), startTask(sagaId, taskId, data), endTask(sagaId, taskId, data), endSaga(sagaId)))
        val saga = (for {
            coord <- SagaCoordinator.createInMemory(existingLogs)
            saga <- coord.recoverSaga(sagaId, ForwardRecovery)
        } yield saga).unsafeRunSync()

        saga.id shouldBe sagaId
        saga.state.completed shouldBe true
        saga.state.aborted shouldBe false
    }

    "Sagas" should "abort" in {
        val (saga, sagaAfter, messages) = (for {
            coord <- SagaCoordinator.createInMemory()
            res <- coord.createSaga(sagaId, data)
            (saga, fiber) = res
            _ <- saga.startTask(taskId, data)
            _ <- saga.abort()
            _ <- saga.startCompensatingTask(taskId, data)
            _ <- saga.endCompensatingTask(taskId, data)
            sagaAfter <- fiber.join
            messages <- coord.log.messages(sagaId)
        } yield (saga, sagaAfter, messages)).unsafeRunSync()

        messages.head shouldBe startSaga(sagaId, data)
        messages(1) shouldBe startTask(sagaId, taskId, data)
        messages(2) shouldBe abortSaga(sagaId)
        messages(3) shouldBe startCompTask(sagaId, taskId, data)
        messages(4) shouldBe endCompTask(sagaId, taskId, data)

        saga.state.isCompTaskCompleted(taskId) shouldBe true
    }
}
