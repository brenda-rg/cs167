package edu.ucr.cs.cs167.broja016 // Do not forget this line

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.io.StdIn

object AppSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167_Lab6_AppSQL")
      .config(conf)
      .getOrCreate()

    val command: String = args(0)
    val inputfile: String = args(1)
    try {
      val input = spark.read.format("csv")
      // TODO 5: -- read input file as a Spark DataFrame
      .option("sep", "\t")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("nasa_19950630.22-19950728.12.tsv") // TODO 5.a
      input.show()

      input.printSchema()
      // TODO 6: print the DataFrame Scheme

      input.createOrReplaceTempView("log_lines")
      // TODO 7: create DataFrame view

      val t1 = System.nanoTime
      var valid_command = true
      command match {

        case "count-all" =>
          // Count total number of records in the file
          val query: String = "SELECT count(*) FROM log_lines;"
          // TODO 8 -- SQL count number of records in input
          val count: Long = spark.sql(query).first().getAs[Long](0)

          //how to run sql query in code
          /*val count = spark.sql(
              """SELECT count(*)
               FROM log_lines""")
            .first()
            .getAs[Long](0)*/

          //alternative
         /* val queryResult = spark sql "SELECT count(*) FROM log_lines" first
          val count = queryResult.getAs[Long](0)*/
          println(s"Total count for file '$inputfile' is $count")
        // TODO 8: count-all SQL

        case "time-filter" => // Filter by time range [from = args(2), to = args(3)], and print the total number of matching lines
          val from: Long = args(2).toLong
          val to: Long = args(3).toLong
          val query: String = s"SELECT count(*) FROM log_lines WHERE time BETWEEN $from AND $to" // TODO 9 --  SQL to filter and count number of records
          val count: Long = spark.sql(query).first().getAs[Long](0)
          println(s"Total count for file '$inputfile' in time range [$from, $to] is $count")
        // TODO 9: time-filter SQL

        case "avg-bytes-by-code" => // Group the liens by response code and calculate the average bytes per group
          println(s"Average bytes per code for the file '$inputfile'")
          println("Code,Avg(bytes)")
          val query: String = "SELECT response, AVG(bytes) FROM log_lines GROUP BY response ORDER BY response;" // TODO 10 -- SQL to get average bytes by response code
          spark.sql(query).collect().foreach(row => println(s"${row.get(0)},${row.get(1)}"))
        // TODO 10: avg-bytes-by-code SQL

        case _ => valid_command = false
      }
      val t2 = System.nanoTime
      if (valid_command) {
        println(s"Command '$command' on file '$inputfile' finished in ${(t2 - t1) * 1E-9} seconds")
        if (!spark.sparkContext.master.startsWith("local")) {
          println("Press Enter to exit (you can access http://localhost:4040 while this is running)...")
          StdIn.readLine()
        }
      }
      else
        Console.err.println(s"Invalid command '$command'")
    } finally {
      spark.stop
    }
  }
}