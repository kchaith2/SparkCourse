/**
  * Created by Krishna Chaithanya on 12-07-2020.
  * Use Case : To load a csv file into an rdd
  */


import org.apache.spark.sql.SparkSession

object broadcast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Broadcasting")
      .master("local")
      .getOrCreate()

    val states = Map(("NY", "New York"), ("CA", "California"), ("FL", "Florida"))
    val countries = Map(("USA", "United States of America"), ("IN", "India"))

    val sc = spark.sparkContext

    val broadcastStates = sc.broadcast(states)
    val broadcastCountries = sc.broadcast(countries)

    val data = Seq(("James", "Smith", "USA", "CA"),
      ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"),
      ("Maria", "Jones", "USA", "FL")
    )

    val columns = Seq("firstname", "lastname", "country", "state")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns: _*)

    df.show()

    val df2 = df.map(row => {
      val country = row.getString(2)
      val state = row.getString(3)

      val fullCountry = broadcastCountries.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (row.getString(0), row.getString(1), fullCountry, fullState)
    }).toDF(columns: _*)

    df2.show(false)
  }
}