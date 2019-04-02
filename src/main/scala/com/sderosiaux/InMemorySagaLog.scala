package com.sderosiaux

import cats.effect.IO
import com.sderosiaux.Saga.SagaId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import cats.implicits._

class InMemorySagaLog(existingLogs: Map[SagaId, List[SagaMessage]] = Map()) extends SagaLog[IO] {

  private val logs = mutable.Map[SagaId, ListBuffer[SagaMessage]]()
  existingLogs.foreach { case (id, msgs) => logs.put(id, msgs.to[ListBuffer]) }

  override def startSaga(sagaId: SagaId, job: Data): IO[Unit] = {
    println(s"Start Saga $sagaId")
    logs.put(sagaId, ListBuffer(SagaMessage.startSaga(sagaId, job)))
    IO.unit
  }

  override def logMessage(message: SagaMessage): IO[Unit] = {
    println(s"Saga ${message.sagaId}: ${message.messageType} ${message.taskId}")
    val saga = logs.get(message.sagaId)
    saga.fold(IO.raiseError[Unit](new Exception("Not started yet"))) { msgs => IO(msgs += message).void }
  }

  override def messages(sagaId: SagaId): IO[List[SagaMessage]] = logs.get(sagaId).fold(
    IO.raiseError[List[SagaMessage]](new Exception("Not such saga"))) { msgs => IO(msgs.toList) }

  override def activeSagas(): IO[List[SagaId]] = IO(logs.keys.toList) // in memory deals only with active sagas

}
