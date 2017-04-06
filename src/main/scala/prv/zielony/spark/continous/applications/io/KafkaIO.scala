package prv.zielony.spark.continous.applications.io

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}


trait KafkaIO {

  protected def fromTopic[K, V](bootstrapServers: Set[String], topics: Set[String])
                                         (implicit session: SparkSession, encoder: Encoder[(K, V)]): Dataset[(K, V)] = {
    session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers.mkString(";"))
      .option("subscribe", topics.mkString(";"))
      .load
      .select("key", "value")
      .as[(K, V)]
  }


  protected implicit class WritableDataset[K, V](stream: Dataset[(K, V)]){
    def toTopic(bootstrapServers: Set[String], topics: Set[String]): StreamingQuery =
      topicSink[K, V](stream, bootstrapServers, topics)
  }

  private[spark] def topicSink[K, V](stream: Dataset[(K, V)], bootstrapServers: Set[String], topics: Set[String]): StreamingQuery =
    stream
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers.mkString(";"))
      .option("subscribe", topics.mkString(";"))
      .outputMode(OutputMode.Append)
      .start
}
