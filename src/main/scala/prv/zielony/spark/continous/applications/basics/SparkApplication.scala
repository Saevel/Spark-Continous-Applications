package prv.zielony.spark.continous.applications.basics

object SparkApplication extends App with Spark {
  sparkSession("SparkApplication", "local[*]")
}
