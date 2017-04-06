package prv.zielony.spark.continous.applications.model

import java.time.LocalDate

import play.api.libs.json._

case class Loan(id: String,
                debtor: Debtor,
                dueDate: LocalDate,
                amount: Double,
                defaultProbability: Double,
                classification: LoanClass,
                status: LoanStatus)

case class LoanClassificationThreshold(min: Double, max: Double, classfication: LoanClass)

sealed trait LoanStatus {
  val name: String
}

object LoanStatus {

  val jsonReads = Reads[LoanStatus] {
    case JsString("ACTIVE") => JsSuccess(Active)
    case JsString("DEFAULTED") => JsSuccess(Defaulted)
    case JsString("PAID") => JsSuccess(Paid)
    case other => JsError(s"Unmapped JSON for LoanStatus: $other.")
  }

  val jsonWrites = Writes[LoanStatus]{
    case Active => JsString("ACTIVE")
    case Defaulted => JsString("DEFAULTED")
    case Paid => JsString("PAID")
  }

  implicit val jsonFormat = Format(jsonReads, jsonWrites)

  object Active extends LoanStatus {
    override val name = "ACTIVE"
  }

  object Defaulted extends LoanStatus{
    override val name = "DEFAULTED"
  }

  object Paid extends LoanStatus {
    override val name = "PAID"
  }
}

sealed trait LoanClass {
  val name: String
}

object LoanClass {

  val jsonReads = Reads[LoanClass]{
    case JsString("SOLID") => JsSuccess(Solid)
    case JsString("HEALTHY") => JsSuccess(Healthy)
    case JsString("NEUTRAL") => JsSuccess(Neutral)
    case JsString("UNHEALTHY") => JsSuccess(Unhealthy)
    case JsString("DANGEROUS") => JsSuccess(Dangerous)
    case other => JsError(s"Unmapped JSON for LoanClass: $other.")
  }

  val jsonWrites = Writes[LoanClass]{
    case Solid => JsString("SOLID")
    case Healthy => JsString("HEALTHY")
    case Neutral => JsString("NEUTRAL")
    case Unhealthy => JsString("UNHEALTHY")
    case Dangerous => JsString("DANGEROUS")
  }

  implicit val jsonFormat = Format(jsonReads, jsonWrites)

  case object Solid extends LoanClass {
    override val name = "SOLID"
  }

  case object Healthy extends LoanClass {
    override val name = "HEALTHY"
  }

  case object Neutral extends LoanClass {
    override val name = "NEUTRAL"
  }

  case object Unhealthy extends LoanClass {
    override val name = "UNHEALTHY"
  }

  case object Dangerous extends LoanClass {
    override val name = "DANGEROUS"
  }
}

case class Debtor(id: String, business: Set[String], country: String);
