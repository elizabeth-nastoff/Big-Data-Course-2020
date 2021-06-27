import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
//import sqlContext.implicits._
import org.apache.spark.sql.types._

object K_Means {

  def main(args: Array[String]): Unit ={

  val conf = new SparkConf().setAppName("KMean").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val input = sc.textFile("states_crime.txt")

    val parsedData = input.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    val num = 2
    val iters = 20
    val clusters = KMeans.train(parsedData, num, iters)

    val Squared_Errors = clusters.computeCost(parsedData)

  }
}