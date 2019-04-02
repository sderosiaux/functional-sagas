package com.sderosiaux

sealed trait SagaMessageType extends Any with Serializable

object SagaMessageType {
  case object StartSaga extends SagaMessageType
  case object EndSaga extends SagaMessageType
  case object AbortSaga extends SagaMessageType
  case object StartTask extends SagaMessageType
  case object EndTask extends SagaMessageType
  case object StartCompTask extends SagaMessageType
  case object EndCompTask extends SagaMessageType
}

