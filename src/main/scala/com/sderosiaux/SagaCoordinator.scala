package com.sderosiaux

import cats.MonadError
import cats.effect._
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
    implicit val cs = cats.effect.IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)
    InMemorySagaLog.create(existingLogs).map(SagaCoordinator(_))
  }
}


case class SagaCoordinator[F[_] : ConcurrentEffect](log: SagaLog[F]) {
  def createSaga(id: SagaId, task: Data): F[Saga[F]] = Saga.create(id, log, task)

  def activeSagas(): F[List[SagaId]] = log.activeSagas()

  // TODO: we recover a State, not a Saga here
  def recoverSaga(id: SagaId, recoveryType: SagaRecoveryType): F[Saga[F]] = {
    for {
      state <- recoverState(id)
      saga <- Saga.rehydrate(id, state, log)
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
      _ <- Either.cond(msgs.nonEmpty, (), new Throwable("No message to recover from")).raiseOrPure[F]
      _ <- Either.cond(msgs.head.messageType == StartSaga, (), new Throwable("First message must be startSaga")).raiseOrPure[F]
      state <- SagaState.create(id, msgs.head.data.get).pure[F]
      newStateOrError = msgs.drop(1).foldLeft(state.asRight[Throwable]) { (oldState, msg) => oldState.flatMap(_.validateAndUpdateForRecoveryNoLog(msg)) }
      newState <- newStateOrError.pure[F].rethrow
    } yield newState
  }


}


