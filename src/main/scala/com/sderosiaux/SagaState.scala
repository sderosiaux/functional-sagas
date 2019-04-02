package com.sderosiaux

import cats.Show
import com.sderosiaux.Saga.{SagaId, TaskId}
import com.sderosiaux.SagaErrors.InvalidSagaStateError
import com.sderosiaux.SagaMessageType._
import com.sderosiaux.TaskState.{CompTaskCompleted, CompTaskStarted, TaskCompleted, TaskStarted}
import monocle.macros.GenLens

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// TODO: pretty sure "Task" is missing (to contain its state and data, and toString ;)

// We can even define a proper ADT instead of a State
sealed trait TaskState
object TaskState {
  case object TaskStarted extends TaskState
  case object TaskCompleted extends TaskState
  case object CompTaskStarted extends TaskState
  case object CompTaskCompleted extends TaskState
}

trait Data
case class TaskData(taskStart: Option[Data], taskEnd: Option[Data], compTaskStart: Option[Data], compTaskEnd: Option[Data])
object TaskData {
  def empty() = TaskData(None, None, None, None)
}

// TODO: the taskState should not be a List but a simple State
case class SagaState(sagaId: SagaId, job: Option[Data], taskData: Map[TaskId, TaskData], taskState: Map[TaskId, List[TaskState]], aborted: Boolean, completed: Boolean) {
  import monocle.macros.GenLens
  import monocle.function.At.at
  val stateLens = GenLens[SagaState](_.taskState)

  def taskIds(): List[TaskId] = taskState.keys.toList
  def isTaskStarted(taskId: TaskId): Boolean = taskState.get(taskId).exists(_.contains(TaskStarted))
  def startTaskData(taskId: TaskId): Option[Data] = taskData.get(taskId).flatMap(_.taskStart)
  def isTaskCompleted(taskId: TaskId): Boolean = taskState.get(taskId).exists(_.contains(TaskCompleted))
  def endTaskData(taskId: TaskId): Option[Data] = taskData.get(taskId).flatMap(_.taskEnd)

  def isCompTaskStarted(taskId: TaskId): Boolean = taskState.get(taskId).exists(_.contains(CompTaskStarted))
  def startCompTaskData(taskId: TaskId): Option[Data] = taskData.get(taskId).flatMap(_.compTaskStart)
  def isCompTaskCompleted(taskId: TaskId): Boolean = taskState.get(taskId).exists(_.contains(CompTaskCompleted))
  def endCompTaskData(taskId: TaskId): Option[Data] = taskData.get(taskId).flatMap(_.compTaskEnd)

  def isSagaInSafeState(): Boolean = aborted || !taskState.keys.exists { taskId => isTaskStarted(taskId) && !isTaskCompleted(taskId) }

  def validateAndUpdateForRecoveryNoLog(msg: SagaMessage): Either[Throwable, SagaState] = {
    validateSagaUpdate(msg).map { _ => processMessage(msg) }
  }

  private def withTaskData(taskId: TaskId, messageType: SagaMessageType, data: Data): SagaState = {
    val tData = taskData.getOrElse(taskId, TaskData.empty())
    val newTaskData = messageType match {
      case SagaMessageType.StartTask => taskData.updated(taskId, tData.copy(taskStart = Some(data)))
      case SagaMessageType.EndTask => taskData.updated(taskId, tData.copy(taskEnd = Some(data)))
      case SagaMessageType.StartCompTask => taskData.updated(taskId, tData.copy(compTaskStart = Some(data)))
      case SagaMessageType.EndCompTask => taskData.updated(taskId, tData.copy(compTaskEnd = Some(data)))
      case _ => ??? // TODO it should not be possible to get there
    }

    copy(taskData = newTaskData)
  }

  def processMessage(msg: SagaMessage): SagaState = msg.messageType match {
    case SagaMessageType.StartSaga => ???
    case SagaMessageType.EndSaga => copy(completed = true)
    case SagaMessageType.AbortSaga => copy(aborted = true)
    case SagaMessageType.StartTask =>  addStateAndData(msg, TaskStarted)
    case SagaMessageType.EndTask => addStateAndData(msg, TaskCompleted)
    case SagaMessageType.StartCompTask => addStateAndData(msg, CompTaskStarted)
    case SagaMessageType.EndCompTask => addStateAndData(msg, CompTaskCompleted)
  }

  private def addStateAndData(msg: SagaMessage, state: TaskState): SagaState = {
    val newState = (stateLens composeLens at(msg.taskId.get)).modify(ts => Some(ts.getOrElse(List()) ++ List(state)))(this)
    msg.data.fold(newState) { d => newState.withTaskData(msg.taskId.get, msg.messageType, d) }
  }

  def validateSagaUpdate(msg: SagaMessage): Either[Throwable, Unit] = msg.messageType match {
    case StartSaga =>
      Left(InvalidSagaStateError("Cannot apply a StartSaga Message to an already existing Saga"))

    case EndSaga =>
      if (aborted) {
        // TODO: fold with error please
        if (taskState.forall { case (taskId, _) => isCompTaskStarted(taskId) && isCompTaskCompleted(taskId) }) {
          Right(())
        } else {
          Left(InvalidSagaStateError(""))
        }
      } else {
        // TODO: fold with error please
        if (taskState.forall { case (taskId, _) => isTaskStarted(taskId) && isTaskCompleted(taskId) }) {
          Right(())
        } else {
          Left(InvalidSagaStateError(""))
        }
      }

    case AbortSaga =>
      if (completed) {
        Left(InvalidSagaStateError("AbortSaga Message cannot be applied to a Completed Saga"))
      } else {
        Right(())
      }

    case StartTask =>
      if (completed)
        Left(InvalidSagaStateError("Cannot StartTask after Saga has been completed"))
      if (aborted)
        Left(InvalidSagaStateError("Cannot StartTask after Saga has been aborted"))
      if (isTaskCompleted(msg.taskId.get)) // TODO: holy shit
        Left(InvalidSagaStateError("Cannot StartTask after it has been completed"))
      Right(())

    case EndTask =>
      if (completed)
        Left(InvalidSagaStateError("Cannot EndTask after Saga has been completed"))
      if (aborted)
        Left(InvalidSagaStateError("Cannot EndTask after Saga has been aborted"))
      if (!isTaskStarted(msg.taskId.get)) // TODO: holy shit
        Left(InvalidSagaStateError("Cannot EndTask before it has been started"))
      Right(())

    case StartCompTask =>
      if (completed)
        Left(InvalidSagaStateError("Cannot StartCompTask after Saga has been completed"))
      if (!aborted)
        Left(InvalidSagaStateError("Cannot StartCompTask after Saga has NOT been aborted"))
      if (isTaskStarted(msg.taskId.get)) // TODO: holy shit
        Left(InvalidSagaStateError("Cannot StartCompTask before a StartTask"))
      if (isCompTaskCompleted(msg.taskId.get)) // TODO: holy shit
        Left(InvalidSagaStateError("Cannot StartCompTask after it has been completed"))
      Right(())

    case EndCompTask =>
      if (completed)
        Left(InvalidSagaStateError("Cannot EndCompTask after Saga has been completed"))
      if (!aborted)
        Left(InvalidSagaStateError("Cannot EndCompTask after Saga has NOT been aborted"))
      if (isTaskStarted(msg.taskId.get)) // TODO: holy shit
        Left(InvalidSagaStateError("Cannot StartCompTask before a StartTask"))
      if (!isCompTaskCompleted(msg.taskId.get)) // TODO: holy shit
        Left(InvalidSagaStateError("Cannot EndCompTask before it has been comp-started"))
      Right(())
  }

}

object SagaState {
  def create(sagaId: SagaId, job: Data): SagaState = {
    SagaState(sagaId, Some(job), Map(), Map(), aborted = false, completed = false)
  }
}
