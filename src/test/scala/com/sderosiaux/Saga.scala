package com.sderosiaux

import com.sderosiaux.SagaMessage._
import com.sderosiaux.SagaRecoveryType.ForwardRecovery
import org.scalatest.{FlatSpec, Matchers}

class SagaTest extends FlatSpec with Matchers {
  val data = new Data {}
  val sagaId = "my saga"
  val taskId = "my lonely task"

  "Saga" should "work" in {
    val coord = SagaCoordinator(new InMemorySagaLog())
    val saga = coord.createSaga(sagaId, data).unsafeRunSync()
    saga.id shouldBe sagaId
  }

  "Sagas" should "be recoverable" in {
    val existingLogs = Map(sagaId -> List(startSaga(sagaId, data), startTask(sagaId, taskId, data), endTask(sagaId, taskId, data), endSaga(sagaId)))
    val coord = SagaCoordinator(new InMemorySagaLog(existingLogs))
    val saga = coord.recoverSaga(sagaId, ForwardRecovery).unsafeRunSync()

    saga.id shouldBe sagaId
    saga.state.completed shouldBe true
    saga.state.aborted shouldBe false
  }

  "Sagas" should "abort" in {
    val existingLogs = Map(sagaId -> List(startSaga(sagaId, data), abortSaga(sagaId)))
    val coord = SagaCoordinator(new InMemorySagaLog(existingLogs))
    val saga = coord.createSaga(sagaId, data).unsafeRunSync()

    saga.abort()

    saga.id shouldBe sagaId
    saga.state.completed shouldBe false
    saga.state.aborted shouldBe true
  }
}
