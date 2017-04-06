package prv.zielony.spark.continous.applications.io

import prv.zielony.spark.continous.applications.basics.Spark

object KafkaIOApplication extends App with KafkaIO with Spark {

    implicit val session = sparkSession("KafkaIOApplication", "local[*]")
    import session.implicits._

    fromTopic[String, Array[Byte]](Set("127.0.0.1:2081"), Set("inputTopic"))
      .map{case(key, value) => (key, value)}
      .filter{_ => true}
      .toTopic(Set("127.0.0.1:2081"), Set("outputTopic"))
}
