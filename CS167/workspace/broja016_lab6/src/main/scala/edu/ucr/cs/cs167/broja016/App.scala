// Remember to change UCRNetID to your net id
package edu.ucr.cs.cs167.broja016

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.io.StdIn

object App {

  def main(args: Array[String]) {
    val command: String = args(0)
    val inputfile: String = args(1)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")
    conf.setAppName("CS167_Lab6_App")
    val sparkContext = new SparkContext(conf)

    try {
      val inputRDD: RDD[String] = sparkContext.textFile(inputfile)

      // TODO 1a: skip the first line
      val validLines: RDD[String] = inputRDD.filter(line => !line.startsWith("host\tlogname"))
      // TODO 1a: filter lines which do not start with "host\tlogname" from `inputRDD`

      // TODO 1b: parse each line
      val parsedLines: RDD[Array[String]] = validLines.map(line => line.split("\t"))
      // TODO 1b: split each line by "\t" from `validLines` via `map`

      val t1 = System.nanoTime
      var valid_command = true
      command match {

        // TODO 2: count-all code
        case "count-all" =>
        // Count total number of records in the file
          val count: Long = parsedLines.count()
          // TODO 2: count total number of records in the file on `parsedLines`
            println(s"Total count for file '$inputfile' is $count")

        // TODO 3: time-filter code
        case "time-filter" =>
          // Filter by time range [from = args(2), to = args(3)], and print the total number of matching lines

          val from: Long = args(2).toLong
          // TODO: read from timestamp
          val to: Long = args(3).toLong
          // TODO read to timestamp

          val filteredLines: RDD[Array[String]] = parsedLines.filter(line =>  {
            val time = line(2).toLong
            time >= from && time <= to
          })
          // TODO 3: `filter` on  `parsedLines` by time (column 2) with `from` and `to`

          val count: Long = filteredLines.count()
          println(s"Total count for file '$inputfile' in time range [$from, $to] is $count")

        // TODO 4: avg-bytes-by-code code
        case "avg-bytes-by-code" =>
          // Group the liens by response code and calculate the average bytes per group
          val loglinesByCode: RDD[(String, Long)] =  parsedLines.map(line => {
            val responseCode = line(5)
            val bytes = try {
              line(6).toLong
            } catch {
              case _: NumberFormatException => 0L
            }
            (responseCode,bytes)
          })
          // TODO 4a: `map` on `parsedLines` by response code (column 5) and bytes (column 6)
          val sums: RDD[(String, Long)] = loglinesByCode.reduceByKey((a,b) => a + b)
          // TODO 4b: `reduceByKey` on `loglinesByCode`
          val counts: Map[String, Long] = loglinesByCode.countByKey()
          // TODO 4c: `countByKey` on `loglinesByCode`
          println(s"Average bytes per code for the file '$inputfile'")
          println("Code,Avg(bytes)")
          sums.sortByKey().collect().foreach(pair => {
            val code = pair._1
            val sum = pair._2
            val count = counts(code)
            println(s"$code,${sum.toDouble / count}")
          })

          /*val sumAndCount: RDD[(String, (Long, Long))] = loglinesByCode.aggregateByKey((0L, 0L))(
            (acc, bytes) => (acc._1 + bytes, acc._2 + 1), // seqOp: Update sum and count per partition
            (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // combOp: Merge results from partitions
          )

          // Compute and print the averages
          println(s"Average bytes per code for the file '$inputfile'")
          println("Code,Avg(bytes)")
          sumAndCount.sortByKey().collect().foreach { case (code, (sum, count)) =>
            println(s"$code,${sum.toDouble / count}")
          }*/

        case _ => valid_command = false
      }
      val t2 = System.nanoTime
      if (valid_command) {
        println(s"Command '$command' on file '$inputfile' finished in ${(t2 - t1) * 1E-9} seconds")
        if(!sparkContext.master.startsWith("local")) {
          println("Press Enter to exit (you can access http://localhost:4040 while this is running)...")
          StdIn.readLine()
        }
      }
      else
        Console.err.println(s"Invalid command '$command'")
    } finally {
      sparkContext.stop
    }
  }
}