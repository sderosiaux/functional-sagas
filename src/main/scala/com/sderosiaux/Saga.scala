package com.sderosiaux

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ThreadFactory}

import cats.MonadError
import cats.effect.concurrent.MVar
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

  def create[F[_] : ConcurrentEffect](id: SagaId, log: SagaLog[F], data: Data): F[Saga[F]] = {
    val state = SagaState.create(id, data)
    for {
        channel <- MVar.empty[F, SagaMessage]
        saga = Saga(id, log, state, channel)
        _ <- log.startSaga(id, data)
        _ <- saga.startLoop()
    } yield saga
  }

  // recreate an existing Saga (no logging)
  def rehydrate[F[_] : ConcurrentEffect](sagaId: SagaId, state: SagaState, log: SagaLog[F]): F[Saga[F]] = {
    for {
        channel <- MVar.empty[F, SagaMessage]
        saga = Saga(sagaId, log, state, channel)
         _ <- if (!saga.state.completed) saga.startLoop() else ().pure[F]
    } yield saga
  }
}

case class Saga[F[_] : ConcurrentEffect](id: SagaId, log: SagaLog[F], var state: SagaState, queue: MVar[F, SagaMessage]) {
  var looping = new AtomicBoolean(true)

  def startLoop(): F[Unit] = {
    for {
        msg <- queue.take
        newSaga <- log(msg) // TODO move out
        _ <- ConcurrentEffect[F].delay(println(newSaga))
        _ <- startLoop()
    } yield ()
  }

  def end(): F[Unit] = {
    enqueueMessage(SagaMessage(id, SagaMessageType.EndSaga))
  }

  def abort(): F[Unit] = {
    enqueueMessage(SagaMessage(id, SagaMessageType.AbortSaga))
  }

  def startTask(taskId: TaskId, data: Data): F[Unit] = {
    enqueueMessage(SagaMessage(id, SagaMessageType.StartTask, Some(data), Some(taskId)))
  }

  def endTask(taskId: TaskId, result: Data): F[Unit] = {
    enqueueMessage(SagaMessage(id, SagaMessageType.EndTask, Some(result), Some(taskId)))
  }

  def startCompensatingTask(taskId: TaskId, data: Data): F[Unit] = {
    enqueueMessage(SagaMessage(id, SagaMessageType.StartCompTask, Some(data), Some(taskId)))
  }

  def endCompensatingTask(taskId: TaskId, result: Data): F[Unit] = {
    enqueueMessage(SagaMessage(id, SagaMessageType.EndCompTask, Some(result), Some(taskId)))
  }

  def enqueueMessage(message: SagaMessage): F[Unit] = {
    queue.put(message)
      // TODO how to stop the queue pulling channel?
    /*message.messageType match {
      case EndSaga => stop.set(true)
      case _ =>
    }*/
  }

  def log(msg: SagaMessage): F[Saga[F]] = for {
    _ <- state.validateSagaUpdate(msg).raiseOrPure[F]
    _ <- log.logMessage(msg)
    saga <- copy(state = state.processMessage(msg)).pure[F]
  } yield saga
}
