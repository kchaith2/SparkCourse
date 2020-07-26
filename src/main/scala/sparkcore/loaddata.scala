
/**
  * Created by Krishna Chaithanya on 12-07-2020.
  * Use Case : To load a csv file into an rdd
  */

package sparkcore

import org.apache.spark.sql.SparkSession

object loaddata {
  def main(args: Array[String]) {

    val masterOfCluster = args(0) //"local"
    val inputPath = args(1) //"src//main//datasets//transactions.csv"

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val transactionRdd = sc.textFile(inputPath,5)

    println(s"No of partitions is ${transactionRdd.getNumPartitions}")


//    val result = rdd.collect().toList

    /*just print 10 records*/
    transactionRdd.foreach(println)

  }
}