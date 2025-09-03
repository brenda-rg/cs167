#!/usr/bin/env sh
mvn clean package
#run filter 200 on nasa_19950801.tsv
yarn jar target/broja016_lab4-1.0-SNAPSHOT.jar edu.ucr.cs.cs167.broja016.Filter nasa_19950801.tsv filter_output 200
#run aggregation 200 on nasa_19950801.tsv
yarn jar target/broja016_lab4-1.0-SNAPSHOT.jar edu.ucr.cs.cs167.broja016.Aggregation nasa_19950801.tsv aggregation_output
#run aggregation on nasa_19950630.22-19950728.12.tsv
yarn jar target/broja016_lab4-1.0-SNAPSHOT.jar edu.ucr.cs.cs167.broja016.Aggregation nasa_19950630.22-19950728.12.tsv aggregation_large_output.tsv
#run filter 200 on nasa_19950630.22-19950728.12.tsv
yarn jar target/broja016_lab4-1.0-SNAPSHOT.jar edu.ucr.cs.cs167.broja016.Filter nasa_19950630.22-19950728.12.tsv filter_large_output.tsv 200
#run aggregation on the filtered file produced above
yarn jar target/broja016_lab4-1.0-SNAPSHOT.jar edu.ucr.cs.cs167.broja016.Aggregation filter_large_output.tsv aggregation_filter_large_output.tsv

read -rn1