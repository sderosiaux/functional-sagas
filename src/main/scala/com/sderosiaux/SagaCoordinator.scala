package com.sderosiaux

import cats.{Monad, MonadError}
import com.sderosiaux.Saga.SagaId
import cats.implicits._
import com.sderosiaux.SagaMessageType.StartSaga
import com.sderosiaux.SagaRecoveryType.{ForwardRecovery, RollbackRecovery}

sealed trait SagaRecoveryType

object SagaRecoveryType {

  case object RollbackRecovery extends SagaRecoveryType

  case object ForwardRecovery extends SagaRecoveryType

}


case class SagaCoordinator[F[_] : MonadError[?[_], Throwable]](log: SagaLog[F]) {
  def createSaga(id: SagaId, task: Data): Saga[F] = Saga.create(id, log, task)

  def activeSagas(): F[List[SagaId]] = log.activeSagas()

  // TODO: we recover a State, not a Saga here
  def recoverSaga(id: SagaId, recoveryType: SagaRecoveryType): F[Saga] = {
    for {
      state <- recoverState(id)
      saga <- Saga.rehydrate(id, state, log).pure[F]
      _ <- recoveryType match {
        case RollbackRecovery => if (!saga.state.isSagaInSafeState()) saga.abort().pure[F] // compensating effect should start
                                 else ().pure[F]
        case ForwardRecovery => ().pure[F]
      }
    } yield saga
  }

  def recoverState(id: SagaId): F[SagaState] = {
    for {
      msgs <- log.messages(id) // TODO: if msgs is empty, Ouch!
      _ <- if (msgs.head.messageType != StartSaga)
        new Exception("First message must be startSaga").raiseError[F, Unit]
      else
        ().pure[F]
      state <- SagaState.create(id, msgs.head.data.get).pure[F] // TODO: Ouch!
      _ <- msgs.drop(1).foreach { msg => state.validateAndUpdate(msg) }.pure[F]
    } yield state
  }
}

