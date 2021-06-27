import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.graphframes._

object Graph_algs {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/progra~1/winutils")
    val spark = SparkSession
      .builder()
      .appName("ICP13")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Import dataset
    val stationsDF = spark.read.option("header", true).csv("station_data.csv")
    val tripDF = spark.read.option("header", true).csv("trip_data.csv")

    val tripDF_3 = tripDF.distinct()
    val stationsDF_3 = stationsDF.distinct()

    // rename column
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

    // create vertices
    val vertices = stationsDF_3.select(col("station_id").as("id"),
      col("name"),
      concat(col("lat"), lit(","), col("long")).as("lat_long"),
      col("dockcount"),
      col("landmark"),
      col("installation"))

    // create edges
    val edges = tripDF_4.select("src", "dst", "Duration", "Trip_Id", "Start_Date", "Start_Station", "End_Date", "End_Station", "Bike_Num", "Subscriber_Type", "Zip_Code")

    //create graph
    val graph = GraphFrame(vertices, edges)



    // 2. Triangle count

    //val tri_results = graph.triangleCount.run()
    //tri_results.select("id", "count").show()



    // 3. Find shortest paths w.r.t landmarks

    //val shortest_path = graph.shortestPaths.landmarks(Seq("5", "16")).run()
    //shortest_path.select("id", "distances").show()



    // 4. Apply page rank algorithm

   /* val results = graph.pageRank.resetProbability(0.15).tol(0.01).run()

    // Run PageRank for a fixed number of iterations.
    val results2 = graph.pageRank.resetProbability(0.15).maxIter(10).run()

    // Run PageRank personalized for vertex "a"
    //val results3 = graph.pageRank.resetProbability(0.15).maxIter(10).sourceId("a").run()

    // Run PageRank personalized for vertex ["a", "b", "c", "d"] in parallel
    //val results4 = graph.parallelPersonalizedPageRank.resetProbability(0.15).maxIter(10).sourceIds(Array("a", "b", "c", "d")).run()

    results.vertices.select("id", "pagerank").show()
    results.edges.select("src", "dst", "weight").show()*/



    // 5. Save graphs generated to a file

    /*graph.vertices.write.parquet("hdfs://localhost:4040/ICP13/vertices")
    graph.edges.write.parquet("hdfs://localhost:4040/ICP13/edges")

    val sameV = spark.read.parquet("hdfs://localhost:4040/ICP13/vertices")
    val sameE = spark.read.parquet("hdfs://localhost:4040/ICP13/edges")

    val sameG = GraphFrame(sameV, sameE)

    sameG.edges.show()
    sameG.vertices.show()*/

    // bonus

    // 1. Label Propagation algorithm

    // 2. BFS algorithm

   /* val paths: DataFrame = graph.bfs.fromExpr("landmark = 'San Jose'").toExpr("dockcount = '11'").run()
    paths.show()

    graph.bfs.fromExpr("landmark = 'San Jose'").toExpr("dockcount = '11'")
      .edgeFilter("name != 'San Jose Civic Center'")
      .maxPathLength(3)
      .run()*/

  }
}
