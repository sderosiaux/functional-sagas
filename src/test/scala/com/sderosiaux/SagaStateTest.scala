package com.sderosiaux

import org.scalatest.{FlatSpec, Matchers}

class SagaStateTest extends FlatSpec with Matchers {
  "SagaState" should "work" in {
    val d = new Data {}
    val state = SagaState.create("toto", d)
    state.sagaId shouldBe "toto"
    state.job shouldBe Some(d)
  }
}
