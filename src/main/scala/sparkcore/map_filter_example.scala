package sparkcore

/**
  * Created by Krishna Chaithanya 12-07-2020.
  * Use case : To load data from file and remove header and take out only the record which has age > 21 and save result
  * Tranformaion used - map & filter
  */

import org.apache.spark.sql.SparkSession

object map_filter_example {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\hadoop")
    val spark = SparkSession.builder().master("local[1]")
      .appName("Example1")
      .getOrCreate()

    val sc = spark.sparkContext

    val myRDD = sc.textFile("src\\main\\datasets\\Customer.csv",5)

    // remove header from RDD
    val filteredRDD =  myRDD.filter(line => !line.startsWith("Id"))

    // take out 2nd  and 3rd element from array of string and filter age > 21
    val res = filteredRDD.map(rec =>  {
      val fields = rec.split(",")
      (fields(1),fields(2).toInt)
    }).filter(t => t._2 > 21)

    res.foreach(println)


    //To save to text file
//    res.coalesce(1).saveAsTextFile("src\\main\\output")

  }
}