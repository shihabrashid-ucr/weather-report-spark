#!/bin/bash
set -e
mvn package

spark-submit --class WeatherReport --master local[8] target/WeatherProject-1.0-SNAPSHOT.jar "$1" "$2" "$3"