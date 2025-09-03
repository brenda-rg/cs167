#!/usr/bin/env sh
mvn clean package

# Part A
spark-submit --class target/edu.ucr.cs.cs167.broja016.App --master "local[*]" target/broja016_lab6-1.0-SNAPSHOT.jar count-all nasa_19950630.22-19950728.12.tsv
spark-submit --class target/edu.ucr.cs.cs167.broja016.App --master "local[*]" target/broja016_lab6-1.0-SNAPSHOT.jar time-filter nasa_19950630.22-19950728.12.tsv 804955673 805590159
spark-submit --class target/edu.ucr.cs.cs167.broja016.App --master "local[*]" target/broja016_lab6-1.0-SNAPSHOT.jar avg-bytes-by-code nasa_19950630.22-19950728.12.tsv

# Part B
spark-submit --class target/edu.ucr.cs.cs167.broja016.AppSQL --master "local[*]" target/broja016_lab6-1.0-SNAPSHOT.jar count-all nasa_19950630.22-19950728.12.tsv
spark-submit --class target/edu.ucr.cs.cs167.broja016.AppSQL --master "local[*]" target/broja016_lab6-1.0-SNAPSHOT.jar time-filter nasa_19950630.22-19950728.12.tsv 804955673 805590159
spark-submit --class target/edu.ucr.cs.cs167.broja016.AppSQL --master "local[*]" target/broja016_lab6-1.0-SNAPSHOT.jar avg-bytes-by-code nasa_19950630.22-19950728.12.tsv
