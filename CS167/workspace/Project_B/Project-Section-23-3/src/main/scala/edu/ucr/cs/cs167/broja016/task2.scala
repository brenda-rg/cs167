package edu.ucr.cs.cs167.broja016
//package edu.ucr.cs.cs167.dvuon016

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.SparkSession

/**
 * Scala examples for Beast
 */
object task2 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("ProjectB2")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "chloropleth-map" =>
        // Load the dataset in the Parquet format.
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("observation")
          val df = sparkSession.read.parquet(inputFile)
          df.printSchema()
        // Run a grouped-aggregate SQL query that computes the total number of observations per ZIP code.
        // This means the sum of the column "OBSERVATION_COUNT".
          val total_observations = sparkSession.sql(
            s"""
            SELECT SUM(OBSERVATION_COUNT) AS total_obs, ZIPCode
            FROM observation
            GROUP BY ZIPCode
            """
          )
          total_observations.show(10)
          total_observations.createOrReplaceTempView("total_obs")
        // Run a second grouped-aggregate SQL query that is very similar to the previous one but
        // adds a filter for the given species. The species is given as a command-line argument.
          val speciesFilter = args(2)
          val filter_by_species = sparkSession.sql(
            s"""
              SELECT SUM(OBSERVATION_COUNT) AS species_obs, ZIPCode
              FROM observation
              WHERE COMMON_NAME = '$speciesFilter'
              GROUP BY ZIPCode
            """
          )
          filter_by_species.show()
          filter_by_species.createOrReplaceTempView("speciesFilter")
        // Join the results of the two queries by ZIP Code and compute the ratio of the observations per ZIP Code.
          val ratio = sparkSession.sql(
            s"""
            SELECT t.ZIPCODE, t.total_obs, s.species_obs, (s.species_obs / t.total_obs) AS ratio
            FROM total_obs t
            LEFT JOIN speciesFilter s ON t.ZIPCode = s.ZIPCode
            """
          )
          ratio.createOrReplaceTempView("ratio")
        // While this query has the result we need, it is still missing the geometry of the ZIP code
        // which is needed to draw the choropleth map.
        // To put the geometry back, we will join with the original ZIP Code dataset as shown below.
        // Load the ZIP Code dataset using Beast and convert it to an RDD.
          val commonNames = sparkSession.sql(
            """
            SELECT DISTINCT COMMON_NAME
            FROM observation
            LIMIT 20
            """
          )
          println("Some common names in the dataset:")
          commonNames.show()

          val zipRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")
          val zipDF = zipRDD.toDataFrame(sparkSession)
          zipDF.createOrReplaceTempView("zipcode_geometry")
        // Join the two datasets using an equi-join query on the attributes ZIPCode and ZCTA5CE10.
          val finalDF = sparkSession.sql(
            s"""
            SELECT r.ZIPCode, z.g, r.ratio
            FROM ratio r
            JOIN zipcode_geometry z ON r.ZIPCode = z.ZCTA5CE10
            """
          )
          finalDF.show()
        // To ensure that a single file is written to the output, you might want to use the function coalesce(1).
        // Store the output as a Shapefile named eBirdZIPCodeRatio.
          finalDF.coalesce(1).write.format("shapefile").save("eBirdZIPCodeRatio")
      }

    } finally {
      sparkSession.stop()
    }
  }
}