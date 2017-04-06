package prv.zielony.spark.continous.applications.manipulations

import org.apache.spark.sql.Encoders
import play.api.libs.json.{JsResult, Json}
import prv.zielony.spark.continous.applications.basics.Spark
import prv.zielony.spark.continous.applications.io.KafkaIO
import prv.zielony.spark.continous.applications.model.{Debtor, Loan, LoanClassificationThreshold}

object StreamManipulationApplication extends App with Spark with KafkaIO with StreamingLogic{

  implicit val session = sparkSession("StreamManipulationApplication", "local[*]")
  import session.implicits._

  import prv.zielony.spark.continous.applications.model.LoanStatus._
  import prv.zielony.spark.continous.applications.model.LoanClass._

  implicit val debtorJsonFormat = Json.format[Debtor]
  implicit val loanJsonFormat = Json.format[Loan]

  implicit val jsResultEncoder = Encoders.javaSerialization(classOf[JsResult[Loan]])
  implicit val tupleEncoder = Encoders.tuple(Encoders.STRING, Encoders.STRING)

  fromTopic[String, String](Set("127.0.0.1:9092"), Set("testTopic"))
    .toLoan
    .predictDefaultProbability
    //.classify(session.read.json("").as[LoanClassificationThreshold])
    .map(loan => (loan.id, Json.stringify(Json.toJson(loan))))
    .toTopic(Set("127.0.0.1:9092"), Set("otherTopic"))
    .awaitTermination

}
