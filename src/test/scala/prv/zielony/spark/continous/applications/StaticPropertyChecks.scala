package prv.zielony.spark.continous.applications

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

trait StaticPropertyChecks extends PropertyChecks {
  protected def forOneOf[X](gen: Gen[X])(f: X => Unit): Unit = gen.sample.fold(forOneOf(gen)(f))(x => f(x))
}
