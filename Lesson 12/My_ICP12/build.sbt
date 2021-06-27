name := "ICP12_B"

version := "0.1"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-graphx" % "2.1.0",

  //would not work
  //"graphframes" % "graphframes" "0.8.0-spark3.0-s_2.12"

)

//needed resolver
resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
