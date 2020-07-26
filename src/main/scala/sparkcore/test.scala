
/**
  * Created by Krishna Chaithanya on 12-07-2020.
  * Use Case : Sample program to read csv file, apply map, store as text file using coalsce
  */

package sparkcore

import org.apache.spark.sql.SparkSession
object test {
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "c:\\hadoop\\bin")
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


    res.coalesce(1).saveAsTextFile("src\\main\\output\\result")
    

  }
}