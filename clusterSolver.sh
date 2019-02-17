#!/bin/bash

export PUZZLE=$1

docker-compose up -d --scale slave=2

#docker-compose down
