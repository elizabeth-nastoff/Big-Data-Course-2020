import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Exam_2_DF {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/Program Files/winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Lesson")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    //makes Schema to make some columns number types
    val Schema1 = StructType(Array(
      StructField("Year", IntegerType, true),
      StructField("Country", StringType, true),
      StructField("Winner", StringType, true),
      StructField("Runners-Up", StringType, true),
      StructField("Third", StringType, true),
      StructField("Fourth", StringType, true),
      StructField("GoalsScored", IntegerType, true),
      StructField("QualifiedTeams", IntegerType, true),
      StructField("MatchesPlayed", IntegerType, true),
      StructField("Attendance", FloatType, true))

    )

    //reads in CSV data set as data frame
    val df_matches = spark.read.option("header", "true").csv("WorldCupMatches.csv")
    val df_players = spark.read.option("header", "true").csv("WorldCupPlayers.csv")
    val df_world_cups = spark.read.option("header", "true").schema(Schema1).csv("WorldCups.csv")

    //df_matches.show()
    //df_players.show()
    //df_world_cups.show()
    //print(df_world_cups.schema)

    // 1. List the stadiums by how many matches they held. Which held the most matches?
    // val counted = df_matches.groupBy('Stadium).count().sort($"count".desc).show()

    // for some reason ~80% of the input data is null for some categories, which is shown a lot for this question

    // 2. Show a list of players and the initials of their team
    //val Q2 = df_players.select("Player Name","Team Initials").show()

    // 3. In which wold cups did the hosting country win?
    //val Q3 = df_world_cups.filter("Country == Winner").show()

    // 4. show the data given for world cups taking place after the year 2000
    //val Q4 = df_world_cups.filter(df_world_cups("Year") > 2000).show

    // 5. show the count, mean, std dev, min, and max attendance for the world cup data
    //val Q5 = df_world_cups.describe("Attendance").show()

    // 6. sort the world cups by the most goals scored
    //val Q6 = df_world_cups.sort($"GoalsScored".desc).show()

    // 7. Which referees have done the most games?
    //val Q7 = df_matches.groupBy('Referee).count().sort($"count".desc).show()

    // 8. show matches with attendance greater than 100,000
    //val Q8 = df_matches.filter("Attendance > 100000").show()

    // 9. How many different teams have participated in FIFA?
    val Q9 = df_matches.select(countDistinct("Home Team Name")).show()

    // 10. what is the average number of matches played for a world cup?
    //val Q10 = df_world_cups.agg(avg("MatchesPlayed")).show()

  }
}

// references

//https://spark.apache.org/docs/2.4.0/api/scala/index.html#org.apache.spark.sql.Dataset
// http://allaboutscala.com/big-data/spark/
// https://spark.apache.org/docs/2.2.0/sql-programming-guide.html
// https://mapr.com/blog/using-apache-spark-dataframes-processing-tabular-data/
// https://docs.microsoft.com/en-us/azure/databricks/spark/latest/dataframes-datasets/introduction-to-dataframes-scala
// https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html