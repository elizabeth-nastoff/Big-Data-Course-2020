import org.apache.spark.{SparkConf, SparkContext}


object Merge_Sort {
  System.setProperty("hadoop.home.dir", "C:/Program Files/winutils")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MergeSort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    def ms(x: List[Int]): List[Int] = {
      val n = x.length / 2
      if (n == 0) x
      else {
        def merge(x: List[Int], y: List[Int]): List[Int] =
          (x, y) match {
            case (Nil, y) => y
            case (x, Nil) => x
            case (x1 :: x2, y1 :: y2) =>
              if (x1 < y1) x1 :: merge(x2, y)
              else y1 :: merge(x, y2)
          }

        val (left, right) = x splitAt (n)
        merge(ms(left), ms(right))
      }
    }

    def make_list(count: Int, Max: Int): Seq[Int] =
      for (i <- 1 to count) yield scala.util.Random.nextInt(Max)

    //Creates 10 random ints of values 1-30

    val rand_list = make_list(10, 30).toList
    val sorted_list = sc.parallelize(Seq(rand_list)).map(ms)

    //print statement
    sorted_list.collect().foreach(println)

  }
}
