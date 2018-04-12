# Spark Benchmark

This project aims at running various benchmarks with the [Apache Spark](https://spark.apache.org/) system.
We focus on relational joins and several linear algebra operations.
We assume the data is stored in a [MonetDB](https://www.monetdb.org/Home) dbfarm.

## Getting Started

### Prerequisites

We assume you have a [MonetDB](https://www.monetdb.org/Home) server running either locally or on a remote machine, where the data is stored.
Please refer to the documentation on the website for setup.

[SBT](https://www.scala-sbt.org/) is required for compilation.

We also suppose you have downloaded Spark 2.3.0.

### Workloads

We assume the dbfarm has a database that contains several tables filled with random double values:

* `trand100x1r` to `trand100x1000000r`: 100 columns, with 1, 10, 100, 1K, 10K, 100K and 1M rows.

* `tnrand10x1000000r` and `t2nrand10x1000000`: 11 columns (first one should be an auto-incremented id, named respectively from `c0` to `c10` and `b0` to `b10`) and 1M rows

The linear regression benchmark also requires the following data

* `stations2017` and `tripdata2017` from BIXI that can be obtained [here](https://www.kaggle.com/aubertsigouin/biximtl) (resp. `Stations_2017.csv` and `OD_2017.csv` files that you have to insert into a database)

* `gmdata2017` obtained from the Google Maps API

### Configuration

See the `submit.conf` file to specify various parameters:

* `sparkDir`: the path where Spark is located

* `sparkNThreads`: the maximum number of threads Spark is allowed to use

* `sparkDriverMemory`: the maximum amount of memory the Spark driver can use

* `sparkEventLogDir`: where Spark will save event logs

* `resultsOutputDir`: where to log the results

### Running

Simply type `$ ./submit.sh --help` to display the application usage.

## Output format

All time values are measured in nanoseconds.

### {load,matrix,vecmult,matmult}.csv

Each line corresponds to a table `trand100x1(0)*r`.
First number is the number of rows (thus ranging from 1e0 to 1e6).
Following are the time values that should be averaged if more than one.
Load micro-benchmark results are separated into load.csv and matrix.csv:
the latter is the load of the dataframe and the former its conversion to a matrix
format.

### {join,joinSQL}.csv

Each line corresponds to a join operation.
First number is the number of columns involved in the join condition, ranging from 1 to 11.
Following are the time values.

### {lr,lr-basic}.csv

First number is an identifier, next are the time values which should be summed up.
The identifiers are:

* 0: feature engineering

* 1: data loading

* 2: model training

* 3: model testing

## Bixi Linear Regression

See [full workflow](wiki/BixiLinearSpark.md) and [basic workflow](wiki/BixiLinearSpark-Basic.md).
