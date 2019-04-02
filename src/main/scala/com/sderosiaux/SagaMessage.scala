package com.sderosiaux

import com.sderosiaux.Saga.{SagaId, TaskId}
import com.sderosiaux.SagaMessageType._

// TODO: remove the subtype and make a proper ADT
case class SagaMessage(sagaId: SagaId, messageType: SagaMessageType, data: Option[Data] = None, taskId: Option[TaskId] = None)

object SagaMessage {
  def startSaga(id: SagaId, job: Data): SagaMessage = SagaMessage(id, StartSaga, Some(job))
  def endSaga(id: SagaId): SagaMessage = SagaMessage(id, EndSaga)
  def abortSaga(id: SagaId): SagaMessage = SagaMessage(id, AbortSaga)
  def startTask(id: SagaId, taskId: TaskId, result: Data): SagaMessage = SagaMessage(id, StartTask, Some(result), Some(taskId))
  def endTask(id: SagaId, taskId: TaskId, result: Data): SagaMessage = SagaMessage(id, EndTask, Some(result), Some(taskId))
  def startCompTask(id: SagaId, taskId: TaskId, result: Data): SagaMessage = SagaMessage(id, StartCompTask, Some(result), Some(taskId))
  def endCompTask(id: SagaId, taskId: TaskId, result: Data): SagaMessage = SagaMessage(id, EndCompTask, Some(result), Some(taskId))
}