#!/usr/bin/env sh
mvn clean package
hadoop jar target/broja016_lab2-1.0-SNAPSHOT.jar file://`pwd`/AREAWATER_broja016.csv hdfs://class-238:9000/user/cs167/hdfscopy1.csv
hadoop jar target/broja016_lab2-1.0-SNAPSHOT.jar hdfs://class-238:9000/user/cs167/copy.csv file://`pwd`/localcopy1.csv
hadoop jar target/broja016_lab2-1.0-SNAPSHOT.jar hdfs://class-238:9000/user/cs167/copy.csv hdfs://class-238:9000/user/cs167/hdfscopy2.csv

read -rn1