
/**
  * Created by Krishna Chaithanya on 12-07-2020.
  * Use Case : To load a csv file into an rdd
  */

package sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object case_class {
  def main (args:Array[String]){
    Logger.getLogger("org").setLevel(Level.INFO)
    val inputPath = "C://employee.csv"

    val spark = SparkSession.builder().master("local[*]").appName("case class example").getOrCreate()
    val sc = spark.sparkContext


    import spark.implicits._

    val df = sc.textFile(inputPath).filter(line => ! line.startsWith("id"))
      .map(x => x.split(",")).map(e => employee(e(0).toInt,e(1), e(2).toInt)).toDF()

    val myRDD = sc.textFile("C://Customer.csv",5)

    // remove header from RDD
    val filteredRDD =  myRDD.filter(line => !line.startsWith("Id"))

    // take out 2nd  and 3rd element from array of string and filter age > 21
    val res = filteredRDD.map(rec =>  {
      val fields = rec.split(",")
      (fields(1),fields(2).toInt)
    }).filter(t => t._2 > 21)

//    res.foreach(println)

    println(res.toDebugString)
    res.foreach(println)

    //
//    df.printSchema()
//    df.show()


  }

  case class employee(id:Int, name:String, age:Int)

}
