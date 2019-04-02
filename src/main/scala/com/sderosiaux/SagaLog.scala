package com.sderosiaux

trait SagaLog[F[_]] {
  import Saga._

  def startSaga(sagaId: SagaId, job: Data): F[Unit]
  def logMessage(message: SagaMessage): F[Unit]
  def messages(sagaId: SagaId): F[List[SagaMessage]]
  def activeSagas(): F[List[SagaId]]
}
