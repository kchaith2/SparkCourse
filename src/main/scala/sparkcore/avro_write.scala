/**
  * Created by Krishna Chaithanya on 12-07-2020.
  * Use Case : To write dataframe to avro format
  */

package sparkcore
import com.databricks.spark.avro._
import org.apache.spark.sql.{SaveMode, SparkSession}

object avro_write {
  def main(args:Array[String]){
    val inputPath = "src\\main\\datasets\\employee.csv"

    val spark = SparkSession.builder().master("local[*]").appName("avro example").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val df = sc.textFile(inputPath).filter(line => ! line.startsWith("id"))
        .map(x => x.split(",")).map(e => employee(e(0).toInt,e(1), e(2).toInt)).toDF()

    df.show()
    df.coalesce(1).write.mode(SaveMode.Append).avro("src\\main\\scala\\sparkcore\\person.avro")

    //To test
    val df_read = spark.read.format("com.databricks.spark.avro").load("D:\\KC\\Training\\SparkCourse\\src\\main\\scala\\sparkcore\\person.avro")
    println("printing saved data from frame : ")
    df_read.show()

}
  case class employee(id:Int, name:String, age:Int)

}
