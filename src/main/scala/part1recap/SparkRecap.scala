package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object SparkRecap {

  // the entry point to the Spark structured API
  val spark = SparkSession
    .builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // read a DF
  val cars: DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars")

  // select
  val usefulcarsData: DataFrame = cars.select(
    col("Name"), // column object
    $"Year" // another column object (needs spark.implicits)
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  val carsWeights: DataFrame = cars.selectExpr("Weight_in_lbs / 2.2")

  // filter
  val europeanCars: DataFrame = cars.where(col("Origin") =!= "USA")

  // aggregations
  val averageHP: DataFrame =
    cars.select(
      avg(col("Horsepower")).as("average_hp")
    ) // sum, mean, stddev, min, max

  // grouping
  val countByOrigin = cars
    .groupBy(col("Origin")) // a RelationalGroupedDataset
    .count()

  // joining
  val guitarPlayers: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bands: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val guitaristsBands: DataFrame =
    guitarPlayers.join(bands, guitarPlayers.col("band") === bands.col("id"))

  /*
    join types
     - inner: only the matching rows are kept
     - left/right/full outer join
     - semi/anti
   */

  // datasets
  case class GuitarPlayer(
      id: Long,
      name: String,
      guitars: Seq[Long],
      band: Long
  )

  // datasets = typed distributed collection of objects
  val guitarPlayersDS: Dataset[GuitarPlayer] =
    guitarPlayers.as[GuitarPlayer] // needs spark.implicits
  guitarPlayersDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCars: DataFrame = spark.sql(
    """
      |select Name from cards where Origin = 'USA'
      |""".stripMargin
  )

  // Spark low-level API: RDDs (Resilient Distributed Datasets)
  val sc = spark.sparkContext
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

  // functional operators
  val doubles: RDD[Int] = numbersRDD.map(_ * 2)

  // RDD -> DF
  val numbersDF: DataFrame =
    numbersRDD.toDF("number") // you lose type info, you get SQL capability

  // RDD -> DS
  val numbersDS: Dataset[Int] =
    spark.createDataset(numbersRDD) // keeps type information + SQL capability

  // DS -> RDD
  val guitarPlayersRDD: RDD[GuitarPlayer] = guitarPlayersDS.rdd

  // DF -> RDD
  val carsRDD = cars.rdd // RDD[Row]

  def main(args: Array[String]): Unit = {
    // showing a DF to the console
    cars.show()
    cars.printSchema()
  }
}
