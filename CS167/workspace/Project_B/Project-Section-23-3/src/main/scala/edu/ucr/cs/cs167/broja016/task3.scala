package edu.ucr.cs.cs167.broja016
//package edu.ucr.cs.cs167.ewang117

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object task3 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context
    val conf = new SparkConf().setAppName("Beast Example")

    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)
    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    // Read the operation and input file from arguments
    val operation: String = args(0)
    val inputFile: String = args(1)

    try {
      // Import Beast features
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        // TASK B_3
        case "temporal_analysis" =>
          // command line arg format: temporal_analysis <input_file> <start_date MM/DD/YYYY> <end_date MM/DD/YYYY>

          val startDate = args(2)
          val endDate = args(3)

          // load the dataset
          val df: Dataset[Row] = sparkSession.read.parquet(inputFile)

          // register dataframe as temporary SQL table
          df.createOrReplaceTempView("observations")

          // convert to MM/dd/yyyy format for SQL query
          val startDateSQL = s"to_date('$startDate', 'MM/dd/yyyy')"
          val endDateSQL = s"to_date('$endDate', 'MM/dd/yyyy')"

          // SQL query to parse dates, aggregate the observations
          val query = s"""
            SELECT COMMON_NAME, SUM(OBSERVATION_COUNT) AS num_observations FROM observations
            WHERE to_date(OBSERVATION_DATE, 'yyyy-MM-dd') BETWEEN $startDateSQL AND $endDateSQL
            GROUP BY COMMON_NAME
          """

          // execute the SQL query
          val result = sparkSession.sql(query)

          // coalesce dataframe into single partition, write to csv
          result.coalesce(1)
            .write
            .option("header", "true")
            .csv("eBirdObservationsTime")

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
