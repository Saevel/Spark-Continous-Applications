package prv.zielony.spark.continous.applications.basics

import org.apache.spark.sql.SparkSession

/**
  * Created by Zielony on 2017-04-02.
  */
trait Spark {

  def sparkSession(appName: String, master: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate
}
