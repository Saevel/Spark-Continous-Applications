package prv.zielony.spark.continous.applications.manipulations

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import play.api.libs.json._
import prv.zielony.spark.continous.applications.model.{Loan, LoanClassificationThreshold}

import scala.util.Success

trait StreamingLogic {

  protected implicit class PotentialLoanDataset(stream: Dataset[(String, String)]) {

    def toLoan(implicit format: Format[Loan],
               encoder: Encoder[JsResult[Loan]],
               evidence: Encoder[Loan],
               e2: Encoder[(String, String)]): Dataset[Loan] =
      stream
        .map { case (_, stringifiedBody) => Json.fromJson[Loan](Json.parse(stringifiedBody)) }
        .filter(_.isSuccess)
        .map { loan => loan.get }
  }

  protected implicit class LoanDataset(stream: Dataset[Loan]) {

    // TODO: Use Spark MLLib + no windowing
    def predictDefaultProbability: Dataset[Loan] = ???

    def classify(thresholds: Dataset[LoanClassificationThreshold])(implicit session: SparkSession): Dataset[Loan] = {
      import session.implicits._
      stream.joinWith(thresholds, $"stream.defaultProbability" >= $"thresholds.min" && $"stream.defaultProbability" <= $"thresholds.max")
            .map{case (loan, classifier) => loan.copy(classification = classifier.classfication)}
    }
  }
}