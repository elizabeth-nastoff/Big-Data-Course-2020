import org.apache.spark.sql.SparkSession

//Source code provided by instructor

object Create_DF {
  def main(args: Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "C:/Program Files/winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Lesson")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.option("header", true).csv("survey.csv")
    import spark.implicits._

    //val df_order = df.orderBy($"Country")
    //df_order.show()

    //val df_group = df.groupBy("treatment").count()
    //df_group.show()

    //val joined = joined.join(df, )


    //val no_Duplicates = df.distinct()
   //println("Original size: "+df.count())
    //println("New size:"+no_Duplicates.count())
    //no_Duplicates.show()


  }
}
