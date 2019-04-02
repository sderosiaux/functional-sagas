package com.sderosiaux

sealed abstract class SagaErrors(fatal: Boolean) extends Exception

object SagaErrors {
  case class InvalidRequestError(s: String) extends SagaErrors(true)
  case class InternalLogError(s: String) extends SagaErrors(true)
  case class InvalidSagaStateError(s: String) extends SagaErrors(true)
  case class InvalidSagaMessageError(s: String) extends SagaErrors(false)
}
