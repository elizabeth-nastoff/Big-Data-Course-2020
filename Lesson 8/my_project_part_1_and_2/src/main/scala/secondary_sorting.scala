import org.apache.spark.{SparkConf, SparkContext}

object secondary_sorting {

  def main(args: Array[String]): Unit ={

    //System.setProperty("Hadoop.home.dir","C:/Program Files/winutils")

    //Scala Spark configuration and context
    val conf = new SparkConf().setAppName("Sorting").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Loads input file
    val input = sc.textFile("dates_input.txt")

    //iterates through lines of data and maps them with year-month as key and temp as value
    //Here the year at iter(0) gets tied to the month at iter(1)
    //iter(2) is the day which doesn't concern us
    //iter(3) is the temp
    val split_data = input.map(_.split(",")).map {iter =>((iter(0)+ "-" + iter(1)), iter(3))}

    //Now the map is grouped and sorted, adding the temperatures for each month into a list
    val list = split_data.groupByKey().mapValues(i => i.toList.sortBy(r=>r))

    //prints output in terminal and creates a new text file for it
    list.collect().foreach(println)
    list.saveAsTextFile("dates_output")
}
}
