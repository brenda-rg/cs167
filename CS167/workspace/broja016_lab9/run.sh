#!/usr/bin/env sh
mvn clean package


spark-submit --class edu.ucr.cs.cs167.broja016.PreprocessTweets target/broja016_lab8-1.0-SNAPSHOT.jar Tweets_1m.json

# Part A
spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-country Tweets_1m.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-country tweets.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-country tweets.parquet
# Part B
spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-lang Tweets_1m.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-lang tweets.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-lang tweets.parquet
# Part C
spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-country-with-lang Tweets_1m.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-country-with-lang tweets.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-country-with-lang tweets.parquet
# Part D
spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar corr Tweets_1m.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar corr tweets.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar corr tweets.parquet
# Part E

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-hashtags tweets.json

spark-submit --class edu.ucr.cs.cs167.broja016.AnalyzeTweets target/broja016_lab8-1.0-SNAPSHOT.jar top-hashtags tweets.parquet
