package com.sderosiaux

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ThreadFactory}

import cats.MonadError
import cats.effect.{ConcurrentEffect, Effect, Sync}
import cats.implicits._
import cats.effect.implicits._
import com.sderosiaux.Saga.{SagaId, TaskId}
import com.sderosiaux.SagaMessageType._

import scala.collection.mutable
import scala.concurrent.ExecutionContext


object Saga {
  type SagaId = String
  type TaskId = String

  def create[F[_] : Effect](id: SagaId, log: SagaLog[F], data: Data): F[Saga[F]] = {

    val state = SagaState.create(id, data)
    val q = mutable.Queue[SagaMessage]()
    val s = Saga(id, log, state, q)

    for {
      _ <- log.startSaga(id, data)
      _ <- s.startLoop().pure[F]
    } yield s
  }

  // recreate an existing Saga (no logging)
  def rehydrate[F[_] : Effect](sagaId: SagaId, state: SagaState, log: SagaLog[F]): Saga[F] = {
    val q = mutable.Queue[SagaMessage]()
    val saga = Saga(sagaId, log, state, q)
    if (!saga.state.completed) {
      saga.startLoop()
    }
    saga
  }
}

case class Saga[F[_] : Effect](id: SagaId, log: SagaLog[F], state: SagaState, queue: mutable.Queue[SagaMessage]) {
  // chan updateCh sagaUpdate
  // mutex: RWMutex

  var looping = new AtomicBoolean(true)

  def startLoop(): Unit = {
    ExecutionContext.global.execute { () =>
      while (looping.get()) {
        if (queue.nonEmpty) {
          log(queue.dequeue()).toIO.unsafeRunSync()
        }
        Thread.sleep(10)
      }
    }
  }

  def end(): Unit = {
    processMessage(SagaMessage(id, SagaMessageType.EndSaga))
  }

  def abort(): Unit = {
    processMessage(SagaMessage(id, SagaMessageType.AbortSaga))
  }

  def startTask(taskId: TaskId, data: Data): Unit = {
    processMessage(SagaMessage(id, SagaMessageType.StartTask, Some(data), Some(taskId)))
  }

  def endTask(taskId: TaskId, result: Data): Unit = {
    processMessage(SagaMessage(id, SagaMessageType.EndTask, Some(result), Some(taskId)))
  }

  def startCompensatingTask(taskId: TaskId, data: Data): Unit = {
    processMessage(SagaMessage(id, SagaMessageType.StartCompTask, Some(data), Some(taskId)))
  }

  def endCompensatingTask(taskId: TaskId, result: Data): Unit = {
    processMessage(SagaMessage(id, SagaMessageType.EndCompTask, Some(result), Some(taskId)))
  }

  def processMessage(message: SagaMessage): Unit = {
    queue.enqueue(message) // Should F[Unit]
    message.messageType match {
      case EndSaga => looping.set(false)
      case _ =>
    }
  }

  def log(msg: SagaMessage): F[Unit] = for {
    _ <- state.validateSagaUpdate(msg).raiseOrPure[F]
    res <- log.logMessage(msg)
    _ <- state.updateState(msg).pure[F] // TODO: should be RT
  } yield res
}
