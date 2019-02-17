# Jumble
A jumble puzzle solver using Spark.

The algorithm follows #2 found here:
* https://en.wikipedia.org/wiki/Jumble_algorithm

where permutations are made from the jumbled words.

It would be interesting to implement algorithm #1 and do a comparison.

# How to Use
Install Docker and Docker Compose:
* https://docs.docker.com/install/linux/docker-ce/ubuntu/
* https://docs.docker.com/compose/install/

First, the program needs to be built once, takes about a minute to run.
* ./package.sh

Next, the program can be run in one of two ways.

Locally with a single argument for the puzzle name (see source/puzzles):
* ./localSolver.sh puzzle[1|2|3|4].json

On a Spark cluster, again same argument:
* ./clusterSolver.sh puzzle[1|2|3|4].json

Can see the cluster at http://localhost:8080

When program is done, shutdown the cluster with (needed even when doing multiple runs):
* docker-compose down

Results for both local and cluster mode can be found in source/puzzles/puzzle[1|2|3|4].json.result once finished.

# Adjustments
To adjust the number of cores, memory, or slaves for local mode, modify:
* localSolver.sh

To adjust the number of cores or memory for cluster mode modify:
* docker-compose.yml

To adjust the number of slaves adjust the scale for the slave service in:
* clusterSolver.sh
