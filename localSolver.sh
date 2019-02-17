#!/bin/bash

export PUZZLE=/source/puzzles/$1

cwd=`pwd`
docker run -it --rm -v $cwd/source:/source -w="/source" -e PUZZLE=$PUZZLE shawks03/spark \
   spark-submit --driver-memory 8g --class "Solver" --master local[4] \
   target/scala-2.11/solver_2.11-1.0.jar
