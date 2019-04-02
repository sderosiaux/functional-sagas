package com.sderosiaux

import java.nio.ByteBuffer

import com.sderosiaux.Saga.{SagaId, TaskId}
import com.sderosiaux.SagaErrors.InvalidSagaStateError
import com.sderosiaux.SagaMessageType._

object Saga {
  type SagaId = String
  type TaskId = String

  def create[F[_]](id: SagaId, log: SagaLog[F], data: Data): Saga[F] = {

    val state = SagaState.create(id, data)
    val res: F[Unit] = log.startSaga(id, data)

    // TODO updateCh := make(chan sagaUpdate, 0); ==> sagaUpdate { sagaMessage, error }
    val s = new Saga(id, log, state)
    s.startLoop()
    s
  }

  // recreate an existing Saga (no logging)
  def rehydrate[F[_]](sagaId: SagaId, state: SagaState, log: SagaLog[F]): Saga[F] = {
    val saga = Saga(sagaId, log, state)
    if (!saga.state.completed) {
      saga.startLoop()
    }
    saga
  }
}

case class Saga[F[_]](id: SagaId, log: SagaLog[F], state: SagaState) {
  // chan updateCh sagaUpdate
  // mutex: RWMutex

  def startLoop(): Unit = {
    val msg: SagaMessage = ???
    log(msg)
  }

  def end(): Unit = {
    state.updateState(SagaMessage(id, SagaMessageType.EndSaga))
  }

  def abort(): Unit = {
    state.updateState(SagaMessage(id, SagaMessageType.AbortSaga))
  }

  def startTask(taskId: TaskId, data: Data): Unit = {
    state.updateState(SagaMessage(id, SagaMessageType.StartTask, Some(data), Some(taskId)))
  }

  def endTask(taskId: TaskId, result: Data): Unit = {
    state.updateState(SagaMessage(id, SagaMessageType.EndTask, Some(result), Some(taskId)))
  }

  def startCompensatingTask(taskId: TaskId, data: Data): Unit = {
    state.updateState(SagaMessage(id, SagaMessageType.StartCompTask, Some(data), Some(taskId)))
  }

  def endCompensatingTask(taskId: TaskId, result: Data): Unit = {
    state.updateState(SagaMessage(id, SagaMessageType.EndCompTask, Some(result), Some(taskId)))
  }

  def processMessage(message: SagaMessage): Unit = {
    message match {
      case EndSaga => // TODO close channel...??
      case _ =>
    }
  }

  // TODO: the main method. composition..
  def log(msg: SagaMessage): F[Unit] = {
    val check: Either[Exception, Unit] = state.validateSagaUpdate(msg)
    val res: F[Unit] = log.logMessage(msg)
    val s: Unit = state.updateState(msg)
  }
}
