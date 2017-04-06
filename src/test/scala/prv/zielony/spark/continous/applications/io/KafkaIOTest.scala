package prv.zielony.spark.continous.applications.io

import java.io.File

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.OutputMode
import org.scalacheck.Gen
import org.scalatest.{Matchers, WordSpec}
import prv.zielony.spark.continous.applications.StaticPropertyChecks
import prv.zielony.spark.continous.applications.basics.Spark

class KafkaIOTest extends WordSpec with StaticPropertyChecks with Matchers with KafkaIO with EmbeddedKafka with Spark {

  private val testTopic = "SAMPLE"

  implicit val serializer = new StringSerializer

  private val randomStringPairs = Gen.nonEmptyListOf(
    for {
      first <- Gen.alphaStr
      second <- Gen.alphaStr
    } yield (first, second)
  )

  "KafkaIO" should {

    "stream events from Kafka topic" in forOneOf(randomStringPairs){ pairs =>

      withRunningKafka {

        val config = implicitly[EmbeddedKafkaConfig]

        implicit val session = sparkSession("KafkaIOTest", "local[*]")
        import session.implicits._
        implicit val tupleEncoder = Encoders.product[(String, String)]

        createCustomTopic(testTopic)

        val topic = fromTopic(Set(s"127.0.0.1:${config.kafkaPort}"), Set(testTopic))

        topic.writeStream
          .format("memory")
          .outputMode(OutputMode.Append)
          .queryName(testTopic)
          .start
          .awaitTermination(3 * 1000)

        pairs.foreach { case(key, value) => publishToKafka(testTopic, key, value)}

        val results = session.sql(s"SELECT * FROM $testTopic").as[(String, String)].collect

        results should contain theSameElementsAs(pairs.dropRight(1))
      }
    }

    "stream events to Kafka topic" ignore forOneOf(randomStringPairs) { pairs =>

      withRunningKafka {

        withFile("test.json"){ file =>
          val config = implicitly[EmbeddedKafkaConfig]

          implicit val session = sparkSession("KafkaIOTest", "local[*]")
          import session.implicits._
          implicit val tupleEncoder = Encoders.product[(String, String)]
          createCustomTopic(testTopic)

          val xyz = session.createDataset(pairs)

          xyz.toTopic(Set(s"127.0.0.1:${config.kafkaPort}"), Set(testTopic))
            .awaitTermination(1000)

          val results = pairs.map{ _ => consumeFirstStringMessageFrom(testTopic)}

          results should contain theSameElementsAs(pairs.map(_._2))

        }
      }
    }
  }

  private def withFile(name: String)(f: File => Unit): Unit = {
    val file = new File(name)
    if(file.exists){
      file.delete
    }
    file.createNewFile
    f(file)
  }

  private def withEmbeddedKafka(f: EmbeddedKafkaConfig => Unit)(implicit config: EmbeddedKafkaConfig): Unit =
    withRunningKafka(f(config))
}
