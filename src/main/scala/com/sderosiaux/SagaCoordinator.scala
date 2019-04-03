package com.sderosiaux

import cats.MonadError
import cats.effect.{Effect, IO, Sync}
import cats.implicits._
import com.sderosiaux.Saga.SagaId
import com.sderosiaux.SagaMessageType.StartSaga
import com.sderosiaux.SagaRecoveryType.{ForwardRecovery, RollbackRecovery}

sealed trait SagaRecoveryType

object SagaRecoveryType {

  case object RollbackRecovery extends SagaRecoveryType

  case object ForwardRecovery extends SagaRecoveryType

}

object SagaCoordinator {
  def createInMemory(existingLogs: Map[SagaId, List[SagaMessage]] = Map()): IO[SagaCoordinator[IO]] = {
    InMemorySagaLog.create(existingLogs).map(SagaCoordinator(_))
  }
}


case class SagaCoordinator[F[_] : Effect](log: SagaLog[F]) {
  def createSaga(id: SagaId, task: Data): F[Saga[F]] = Saga.create(id, log, task)

  def activeSagas(): F[List[SagaId]] = log.activeSagas()

  // TODO: we recover a State, not a Saga here
  def recoverSaga(id: SagaId, recoveryType: SagaRecoveryType): F[Saga[F]] = {
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

  private def recoverState(id: SagaId): F[SagaState] = {
    for {
      msgs <- log.messages(id)
      _ <- Effect[F].errorIf(msgs.isEmpty)(new Exception("No message to recover from"))
      _ <- Effect[F].errorIf(msgs.head.messageType != StartSaga)(new Exception("First message must be startSaga"))
      state <- SagaState.create(id, msgs.head.data.get).pure[F]
      newStateOrError = msgs.drop(1).foldLeft(state.asRight[Throwable]) { (oldState, msg) => oldState.flatMap(_.validateAndUpdateForRecoveryNoLog(msg)) }
      newState <- newStateOrError.pure[F].rethrow
    } yield newState
  }

  implicit class RichMonadError[F[_], E](M: MonadError[F, E]) {
    def errorIf(cond: Boolean)(e: E): F[Unit] = {
      if (cond) {
        M.raiseError[Unit](e)
      } else {
        M.pure(())
      }
    }
  }

}


