package com.sderosiaux

import cats.Show
import com.sderosiaux.Saga.{SagaId, TaskId}
import com.sderosiaux.SagaErrors.InvalidSagaStateError
import com.sderosiaux.SagaMessageType._
import com.sderosiaux.TaskState.{CompTaskCompleted, CompTaskStarted, TaskCompleted, TaskStarted}

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
case class SagaState(sagaId: SagaId, job: Option[Data], taskData: mutable.Map[TaskId, TaskData], taskState: mutable.Map[TaskId, mutable.ListBuffer[TaskState]], var aborted: Boolean, var completed: Boolean) {

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

  // TODO: Unit
  def addTaskData(taskId: TaskId, messageType: SagaMessageType, data: Data): Unit = {

    val tData = taskData.getOrElseUpdate(taskId, TaskData.empty())

    messageType match {
      // TODO: OH SHIT! mutability + non exhaustivity
      case SagaMessageType.StartTask => taskData.update(taskId, tData.copy(taskStart = Some(data)))
      case SagaMessageType.EndTask => taskData.update(taskId, tData.copy(taskEnd = Some(data)))
      case SagaMessageType.StartCompTask => taskData.update(taskId, tData.copy(compTaskStart = Some(data)))
      case SagaMessageType.EndCompTask => taskData.update(taskId, tData.copy(compTaskEnd = Some(data)))
      case _ => sys.error("oops")
    }
  }

  def validateAndUpdate(msg: SagaMessage): Either[Throwable, Unit] = {
    val res = validateSagaUpdate(msg)
    res.foreach { _ => processMessage(msg) } // TODO: OMG!
    res
  }

  // TODO: should be pure: returns SagaMessage and remove addTaskData
  def processMessage(msg: SagaMessage): Unit = msg.messageType match {
    case SagaMessageType.EndSaga => completed = true
    case SagaMessageType.AbortSaga => aborted = true

    case SagaMessageType.StartTask =>
      taskState(msg.taskId.get) = ListBuffer(TaskStarted)
      msg.data.foreach { d => addTaskData(msg.taskId.get, msg.messageType, d)}

    case SagaMessageType.EndTask =>
      taskState(msg.taskId.get) += TaskCompleted
      msg.data.foreach { d => addTaskData(msg.taskId.get, msg.messageType, d)}

    case SagaMessageType.StartCompTask =>
      taskState(msg.taskId.get) += CompTaskStarted
      msg.data.foreach { d => addTaskData(msg.taskId.get, msg.messageType, d)}

    case SagaMessageType.EndCompTask =>
      msg.data.foreach { d => addTaskData(msg.taskId.get, msg.messageType, d)}
      taskState(msg.taskId.get) += CompTaskCompleted

    case _ => ???
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
    SagaState(sagaId, Some(job), mutable.Map(), mutable.Map(), aborted = false, completed = false)
  }
}
