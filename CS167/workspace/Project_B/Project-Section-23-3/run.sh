#!/usr/bin/env sh
mvn clean package

#Task 1: Data Preparation (Brenda)
beast --conf spark.executor.memory=30g --conf spark.driver.memory=30g --class edu.ucr.cs.cs167.broja016.BeastScala target/Project-Section-23-3-1.0-SNAPSHOT.jar data-prep eBird_1k.csv
#Task 2 Spatial Analysis (Dylan)
beast --conf spark.executor.memory=30g --conf spark.driver.memory=30g --class edu.ucr.cs.cs167.broja016.task2 target/Project-Section-23-3-1.0-SNAPSHOT.jar chloropleth-map eBird_ZIP Verdin
#Task 3 Temporal Analysis (Eunice)
beast --conf spark.executor.memory=30g --conf spark.driver.memory=30g --class edu.ucr.cs.cs167.broja016.task3 target/Project-Section-23-3-1.0-SNAPSHOT.jar temporal_analysis eBird_ZIP 01/01/1974 12/31/1974