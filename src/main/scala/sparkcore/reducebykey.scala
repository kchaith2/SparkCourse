package sparkcore

/**
  * Created by Pankaj Gaur on 06-07-2020.
  */

import org.apache.spark.sql.SparkSession

object reducebykey {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[*]")
      .appName("Example1")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.textFile("src//main//datasets//myfile.txt",1)

    println(rdd.getNumPartitions)


    //Sort by key requires 1 partition and will run into errors if more partitions
    //else it will give Exception in thread "main" java.lang.IllegalArgumentException

    val m = rdd.flatMap(line => {
      line.split(" ")
    }).map(word => (word,1))
      .filter(t=> ! t._1.startsWith("a"))
      .reduceByKey( (x,y) => x+y )
      .map(t => (t._1,t._2))
      .sortByKey(true, 1)

      print(rdd.collect().mkString("~~~~~~~~~~"))
      print(m.toDebugString)


    // map partitions examples

    val numbers  = sc.parallelize(Seq('A','B','C','D','E', 'F','G','H', 'I'),3)

    numbers.map(x => {
      println("inside map function")
      (x, "hello")
    }).collect().foreach(println)

    numbers.mapPartitions(iter =>{
      println("Inside Map Partitions")
      (List(iter.next).iterator)
    }).collect().foreach(println)


    val rdd1 =  sc.parallelize(List("yellow", "red", "blue", "cyan", "black"), 3)
    val rdd2 =  sc.parallelize(List("yellow", "red", "blue", "cyan", "black"), 3)


    val mapped =   rdd1.mapPartitionsWithIndex{
      // 'index' represents the Partition No
      // 'iterator' to iterate through all elements
      //  in the partition
      (index, iterator) => {
        println("Called in Partition -> " + index + " with elements " + iterator.toList)
        val myList = iterator.toList
        myList.map(x => x + " -> " + index).iterator
      }
    }

    mapped.collect().foreach(println)

//     map partitions examples

  }
}