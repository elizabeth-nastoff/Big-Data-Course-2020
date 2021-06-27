import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

import org.graphframes._

object SparkGraphFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/progra~1/winutils")
    val spark = SparkSession
      .builder()
      .appName("Lesson12")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Import dataset
    val stationsDF = spark.read.option("header", true).csv("station_data.csv")
    val tripDF = spark.read.option("header", true).csv("trip_data.csv")

    // 2. concatenate
    //tripDF.select(concat(col("Start Date"), lit(","), col("End Date")).as("Trip_EndPoints")).show()

    // 3. remove duplicates
    val tripDF_3 = tripDF.distinct()
    val stationsDF_3 = stationsDF.distinct()

    // 4. rename column
    val tripDF_4 = tripDF_3
      .withColumnRenamed("Trip ID", "Trip_ID")
      .withColumnRenamed("Start Date", "Start_Date")
      .withColumnRenamed("Start Station", "Start_Station")
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Date", "End_Date")
      .withColumnRenamed("End Station", "End_Station")
      .withColumnRenamed("End Terminal", "dst")
      .withColumnRenamed("Bike #", "Bike_Num")
      .withColumnRenamed("Subscriber Type", "Subscriber_Type")
      .withColumnRenamed("Zip Code", "Zip_Code")

    // 5. output
    //tripDF_4.show()
    //stationsDF_3.show()

    //val tripDF_5 = tripDF_4.selectExpr("cast(Duration as int) Duration")



    // 6. create vertices
    val vertices = stationsDF_3.select(col("station_id").as("id"),
      col("name"),
      concat(col("lat"), lit(","), col("long")).as("lat_long"),
      col("dockcount"),
      col("landmark"),
      col("installation"))

    val edges = tripDF_4.select("src", "dst", "Duration", "Trip_Id", "Start_Date", "Start_Station", "End_Date", "End_Station", "Bike_Num", "Subscriber_Type", "Zip_Code")

    val graph = GraphFrame(vertices, edges)

    //7 & 8. show vertices and edges
    //graph.vertices.show()
    //graph.edges.show()

    // 9 & 10. vertex in and out degrees
    //graph.inDegrees.show()
    //graph.outDegrees.show()

    //11. display motif findings
    //val motifs = graph.find("(a)-[ab]->(b); (b)-[bc]->(c)")
    //motifs.show()

    //12. Stateful Querie

    val Motifs2 = graph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

    def sum_Q(cnt: Column, End_Station: Column): Column = {
      when(End_Station === "2nd at South Park", cnt + 1).otherwise(cnt)
    }

    val condition = Seq("ab", "bc", "cd").
      foldLeft(lit(0))((cnt, e) => sum_Q(cnt, col(e)("End_Station")))

    val result = Motifs2.where(condition >= 2)

   // result.show()

    val New_Graph = graph
      .filterEdges("Subscriber_Type = 'Subscriber'")
      .filterVertices("landmark = 'San Jose'")
        .dropIsolatedVertices()

    New_Graph.vertices.show()
    New_Graph.edges.show()
  }
}