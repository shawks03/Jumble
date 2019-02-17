#!/bin/bash

cwd=`pwd`
docker run -it --rm -v $cwd/source:/source -w="/source" shawks03/spark sbt package
