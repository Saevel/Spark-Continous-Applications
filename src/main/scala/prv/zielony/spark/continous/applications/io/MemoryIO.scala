package prv.zielony.spark.continous.applications.io

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

// TODO: Test
trait MemoryIO {

  protected def fromTable[T](name: String)(implicit encoder: Encoder[T], session: SparkSession): Dataset[T] =
    fromQuery(s"SELECT * FROM $name")

  // TODO: actual param names
  protected def fromQuery[T](sql: String)(implicit session: SparkSession, encoder: Encoder[T]): Dataset[T] =
    session.readStream.format("memory").option("query", sql).load.as[T]

  protected implicit class ToTableStream[T](dataset: Dataset[T]){
    def toTable[T](name: String): StreamingQuery = toTableStream(dataset, name)
  }

  private[io] def toTableStream[T](dataset: Dataset[T], name: String): StreamingQuery =
    dataset.writeStream.format("memory").queryName(name).outputMode(OutputMode.Append).start

}
