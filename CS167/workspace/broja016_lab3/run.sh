#!/usr/bin/env sh
mvn clean package
hadoop jar broja016_lab3-1.0-SNAPSHOT.jar nasa_19950630.22-19950728.12.tsv 1500 2000
hadoop jar broja016_lab3-1.0-SNAPSHOT.jar nasa_19950630.22-19950728.12.tsv 13245 3500
hadoop jar broja016_lab3-1.0-SNAPSHOT.jar nasa_19950630.22-19950728.12.tsv 112233 4000