package edu.ucr.cs.cs167.broja016

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("ProjectB1")
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
        //Task 1: Data preparation
        //The first step is to prepare the data for processing.
        //This includes two major steps.
        // First, introduce a new attribute ZIPCode that indicates the ZIP code at which each observation happened.
        // Second, convert the file into a column-oriented Parquet format to speed up the analysis.
        case "data-prep" =>
          //val outputFile = args(2)
          // 1. Parse and load the CSV file using the Dataframe API.
          val observationsDF: DataFrame = sparkSession.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(inputFile)
          // 2. Introduce a geometry attribute that represents the location of each observation.
          // Use the ST_CreatePoint function.
          // 3. Keep only the following columns to reduce the size of the dataset:
          // {"x", "y", "GLOBAL UNIQUE IDENTIFIER", "CATEGORY", "COMMON NAME", "SCIENTIFIC NAME", "OBSERVATION COUNT", "OBSERVATION DATE"}
          val filteredDF = observationsDF
            .select("x", "y", "GLOBAL UNIQUE IDENTIFIER", "CATEGORY", "COMMON NAME", "SCIENTIFIC NAME", "OBSERVATION COUNT", "OBSERVATION DATE")
            .selectExpr("*", "ST_CreatePoint(x, y) AS geometry");

          // 4. Rename all attributes that include a space to make them easier to process and compatible with the Parquet format.
          // To do that, use the function withColumnRenamed on the Dataframe.
          val renamedDF = filteredDF.withColumnRenamed("GLOBAL UNIQUE IDENTIFIER", "GLOBAL_UNIQUE_IDENTIFIER")
            .withColumnRenamed("COMMON NAME", "COMMON_NAME")
            .withColumnRenamed("SCIENTIFIC NAME", "SCIENTIFIC_NAME")
            .withColumnRenamed("OBSERVATION COUNT", "OBSERVATION_COUNT")
            .withColumnRenamed("OBSERVATION DATE", "OBSERVATION_DATE");
          //println("BEFORE:")
          //renamedDF.show()
          // 5. Convert the resulting Dataframe to a SpatialRDD to prepare for the next step.
          val observationsRDD: SpatialRDD = renamedDF.toSpatialRDD
          // 6. Load the ZIP Code dataset using Beast.
          val ZIPCodesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")
          // 7. Run a spatial join query to find the ZIP code of each observation.
          val observationZIPCodeRDD: RDD[(IFeature, IFeature)] = ZIPCodesRDD.spatialJoin(observationsRDD)
          // 8. Use the attribute ZCTA5CE10 from the ZIP code to introduce a new attribute ZIPCode in the observations.
          val observationZIPCode: DataFrame = observationZIPCodeRDD.map({ case (zip, observation) => Feature.append(observation, zip.getAs[String]("ZCTA5CE10"), "ZIPCode") })
            .toDataFrame(sparkSession)
          //observationZIPCode.show()
          //observationZIPCode.printSchema()
          // 9. Convert the result back to a Dataframe.
          val convertedDF: DataFrame = observationZIPCode.drop("geometry")
          // 10. Drop the geometry column using the function dropColumn on the Dataframe.
          // 11. Write the output as a Parquet file named eBird_ZIP.
          convertedDF.write.mode(SaveMode.Overwrite).parquet("eBird_ZIP")
          // 12. The final output will have a schema that looks like the following.
          //println("Final Schema:")
          //convertedDF.show(500)
          //convertedDF.printSchema()

        case _ => validOperation = false
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}