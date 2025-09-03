#!/usr/bin/env sh
mvn clean package
#run filter 302 on nasa_19950630.22-19950728.12.tsv
spark-submit --class edu.ucr.cs.cs167.broja016.Filter target/broja016_lab5-1.0-SNAPSHOT.jar nasa_19950630.22-19950728.12.tsv spark_filter_large_output_broja016 302
#run aggregation on nasa_19950630.22-19950728.12.tsv
spark-submit --class edu.ucr.cs.cs167.broja016.Aggregation target/broja016_lab5-1.0-SNAPSHOT.jar nasa_19950630.22-19950728.12.tsv

read -rn1