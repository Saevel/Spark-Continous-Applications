package prv.zielony.spark.continous.applications.advanced

import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import play.api.libs.json.{Format, JsResult, Json, Reads}
import prv.zielony.spark.continous.applications.model.{Loan, LoanClassificationThreshold}

/**
  * Created by Zielony on 2017-04-04.
  */
trait AdvancedStreamingLogic {

  protected implicit class PotentialLoanDataset(stream: Dataset[(String, String)]){

    def toLoan(implicit format: Format[Loan], encoder: Encoder[JsResult[Loan]], evidence: Encoder[Loan]): Dataset[Loan] =
      stream
        .map { case (_, stringifiedBody) => Json.fromJson[Loan](Json.parse(stringifiedBody)) }
        .filter(_.isSuccess)
        .map(_.get)
  }

  protected implicit class LoanDataset(stream: Dataset[Loan]) {

    val regression = new StreamingLinearRegressionWithSGD();

    // TODO: Use Spark MLLib + Time Window
    def predictDefaultProbability: Dataset[Loan] = ???

    def classify(thresholds: Dataset[LoanClassificationThreshold])(implicit session: SparkSession): Dataset[Loan] = {
      import session.implicits._
      stream.joinWith(thresholds, $"stream.defaultProbability" >= $"thresholds.min" && $"stream.defaultProbability" <= $"thresholds.max")
        .map{case (loan, classifier) => loan.copy(classification = classifier.classfication)}
    }
  }
}
