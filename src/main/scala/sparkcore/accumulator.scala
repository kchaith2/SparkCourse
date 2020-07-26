package sparkcore

/**
  * Created by Krishna Chaithanya on 12-07-2020.
  * Use Case : To print bad records using accumulator
  */


import org.apache.spark.sql.SparkSession

object accumulator {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Accumalator")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataRDD = sc.textFile("src\\main\\datasets\\sales-error.csv")

    val badRecords = sc.accumulator(0, "Blank Lines")
    dataRDD.foreach(record =>
      if (record.length() == 0) badRecords += 1
    )
    println("No of bad records is =  " + badRecords.value)

  }
}