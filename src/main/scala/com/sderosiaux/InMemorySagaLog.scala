package com.sderosiaux

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import com.sderosiaux.Saga.SagaId

class InMemorySagaLog(logs: Ref[IO, Map[SagaId, List[SagaMessage]]]) extends SagaLog[IO] {

  override def startSaga(sagaId: SagaId, job: Data): IO[Unit] = {
    IO(println(s"Start Saga $sagaId")) *>
      logs.modify { m => (m.updated(sagaId, List(SagaMessage.startSaga(sagaId, job))), ()) }
  }

  override def logMessage(message: SagaMessage): IO[Unit] = {
    IO(println(s"Saga ${message.sagaId}: ${message.messageType} ${message.taskId}")) *>
      (for {
        saga <- logs.get
        opt = saga.get(message.sagaId)
        newMessages <- opt.fold(IO.raiseError[List[SagaMessage]](new Exception("Not started yet"))) { msgs => IO(msgs ++ List(message)) }
        _ <- logs.modify { m => (m.updated(message.sagaId, newMessages), ()) }
      } yield ())
  }

  override def messages(sagaId: SagaId): IO[List[SagaMessage]] = for {
    map <- logs.get
    msgs <- map.get(sagaId).fold(IO.raiseError[List[SagaMessage]](new Exception("Not such saga"))) { IO(_) }
  } yield msgs

  override def activeSagas(): IO[List[SagaId]] = logs.get.map { _.keys.toList }

}

object InMemorySagaLog {
  def create(existingLogs: Map[SagaId, List[SagaMessage]] = Map()): IO[InMemorySagaLog] = for {
    ref <- Ref[IO].of(existingLogs)
    log = new InMemorySagaLog(ref)
  } yield log
}