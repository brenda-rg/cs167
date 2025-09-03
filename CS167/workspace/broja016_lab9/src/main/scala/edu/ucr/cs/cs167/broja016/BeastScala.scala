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

    val conf = new SparkConf().setAppName("Beast Example")
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
        case "count-by-county" =>
        // Sample program arguments: count-by-county Tweets_1k.tsv
        // TODO count the total number of tweets for each county and display on the screen
          val tweetsDF: DataFrame = sparkSession.read
            .option("sep", "\t")
            .option("header", "true")
            .csv(inputFile)
          //tweetsDF.show()
          //print schema of the DF
          //tweetsDF.printSchema()
          // Create a geometry column from Longitude and Latitude

          //To run spatial operations, we need to combine the two columns related to spatial properties:
          //Longitude and Latitude.
          //Create a new column with type geometry by using Longitude and Latitude
          //tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry")
          //use Beast to convert this new Dataframe to a spatial RDD
          val tweetsRDD: SpatialRDD = tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry").toSpatialRDD
          //load the counties dataset
          val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")
          //combine both RDDs together based on their geospatial location.
          val countyTweet: RDD[(IFeature, IFeature)] = countiesRDD.spatialJoin(tweetsRDD)
          //select the county name and count by name.
          val tweetsByCounty: Map[String, Long] = countyTweet
            .map({ case (county, tweet) => (county.getAs[String]("NAME"), 1) })
            .countByKey()
          //print the output using the following code
          println("County\tCount")
          for ((county, count) <- tweetsByCounty)
            println(s"$county\t$count")

        case "convert" =>
          val outputFile = args(2)
        // TODO add a CountyID column to the tweets, parse the text into keywords, and write back as a Parquet file
            //Load the Tweets dataset as a CSV directly as SpatialRDD this time
          val tweetsRDD: SpatialRDD = sparkContext.readCSVPoint(inputFile,"Longitude","Latitude",'\t')
            //Load the counties dataset as done in the previous part.
            // Alternatively, you can also load it as a DataFrame and then convert it into SpatialRDD as follows
          val countiesDF = sparkSession.read.format("shapefile").load("tl_2018_us_county.zip")
          val countiesRDD: SpatialRDD = countiesDF.toSpatialRDD
            //join the two RDDs together as follows
          val tweetCountyRDD: RDD[(IFeature, IFeature)] = tweetsRDD.spatialJoin(countiesRDD)
            //add a new column named CountyID to each tweet that is equal to the GEOID column in the corresponding county
          val tweetCounty: DataFrame = tweetCountyRDD.map({ case (tweet, county) => Feature.append(tweet, county.getAs[String]("GEOID"), "CountyID") })
            .toDataFrame(sparkSession)
            //see schema
          //tweetCounty.printSchema()
            //parse the tweet Text into an array of keywords to make it easier to analyze
            //Note: geometry, Latitude, and Longitude columns are dropped since we have already wrap the spatial property.
          val convertedDF: DataFrame = tweetCounty.selectExpr("CountyID", "split(lower(text), ',') AS keywords", "Timestamp")
          //convertedDF.printSchema()
            //write the converted Dataframe in Parquet format
          convertedDF.write.mode(SaveMode.Overwrite).parquet(outputFile)

        case "count-by-keyword" =>
          val keyword: String = args(2)
        // TODO count the number of occurrences of each keyword per county and display on the screen
          //Read the parquet file and create a view with name tweets
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("tweets")
          //Run a SQL query to count the number of tweets for each county containing a user-provided keyword
          println("CountyID\tCount")
          sparkSession.sql(
            s"""
              SELECT CountyID, count(*) AS count
              FROM tweets
              WHERE array_contains(keywords, "$keyword")
              GROUP BY CountyID
            """).foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))


        case "choropleth-map" =>
          val keyword: String = args(2)
          val outputFile: String = args(3)
        // TODO write a Shapefile that contains the count of the given keyword by county
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("tweets")

          //count the number of tweets for each county containing a user-provided keyword
          sparkSession.sql(
            s"""
              SELECT CountyID, count(*) AS count
              FROM tweets
              WHERE array_contains(keywords, "$keyword")
              GROUP BY CountyID
            """).createOrReplaceTempView("keyword_counts")
            //Load the county dataset as a Dataframe
          sparkSession.read.format("shapefile").load("tl_2018_us_county.zip")
            .createOrReplaceTempView("counties")
            //Run the following SQL query to join the two datasets and bring back the county name and geometry.
            // Notice that the geometry attribute is named geometry when loaded from the county Shapefile.
          //get total count of keyword for each county
          //get the ratio of keyword/total



          sparkSession.sql(
            s"""
              WITH total_tweets AS (
                SELECT CountyID, COUNT(*) AS total_count
                FROM tweets
                GROUP BY CountyID
              )
              SELECT keyword_counts.CountyID, counties.NAME, counties.geometry, keyword_counts.count AS count,
                     (count / t.total_count) AS keyword_ratio
              FROM keyword_counts
              JOIN total_tweets t ON keyword_counts.CountyID = t.CountyID
              JOIN counties ON keyword_counts.CountyID = counties.GEOID
            """).toSpatialRDD.coalesce(1).saveAsShapefile(outputFile)


          //non-normalized query
          /*sparkSession.sql(
            s"""
              SELECT CountyID, NAME, geometry, count
              FROM keyword_counts, counties
              WHERE CountyID = GEOID
            """).toSpatialRDD.coalesce(1).saveAsShapefile(outputFile)*/

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