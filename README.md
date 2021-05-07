Coreset-based clustering
========================

This code implements algorithms described in "[Improved MapReduce and Streaming Algorithms for _k_-Center Clustering (with Outliers)](https://arxiv.org/abs/1802.09205)".

To compile it you will need [sbt](https://www.scala-sbt.org/) and Java version 8.
The code is based on Spark, which has problems running on Java 9 (at least as of April 2018, see [this issue](https://issues.igniterealtime.org/browse/SPARK-2017)).

To build the code the following command is sufficient

    sbt compile

If you want a "fat jar" suitable for deployment on a Spark cluster, use the following command

    sbt assembly

Insteaed, if you just want to test the software locally, just use the provided `run` script.

Running the program
--------------------

To see all the available options:

    ./run Main --help

The main parameters are the following:

 - `k`: controls the desired number of clusters
 - `z`: controls the number of outliers allowed. If not provided, the software performs a clustering without outliers.
 - `p`: the number of blocks in which to partition the input (only used with the `mapreduce` and `random` coresets, see below)
 - `tau`: the size of each coreset
 - `input`: path to the input file
 - `coreset`: you can choose the coreset to use among the following ones
   - `none`: just run the sequential algorithm. *Beware*: if you also specify `z`, pay attention to use only small inputs, since the sequential algorithm with `z` outliers is cubic.
   - `mapreduce`: use the MapReduce coreset-based algorithm
   - `streaming`: use the streaming coreset-based algorithm
   - `random`: build a coreset obtained by sampling points at random from each partition in MapReduce

Preparing datasets
------------------

In the paper we use the following two datasets as a benchmark:

  - [Higgs](https://archive.ics.uci.edu/ml/datasets/HIGGS)
  - [Power](https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption)

Both datasets need some preprocessing to be used as input to the software.

### Preparing the Higgs dataset

    wget https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz
    cat HIGGS.csv.gz | gunzip | cut -d ',' -f 23,24,25,26,27,28,29 | gzip > Higgs.csv.gz
    ./run VectorIO Higgs.csv.gz Higgs.bin

### Preparing the Power dataset

    wget https://archive.ics.uci.edu/ml/machine-learning-databases/00235/household_power_consumption.zip
    unzip household_power_consumption.zip
    cat household_power_consumption.txt | sed 1d | cut -d ';' -f 1,2 --complement | sed "s/;/,/g" > household_power_consumption.csv
    ./run VectorIO household_power_consumption.csv power.bin

### Adding outliers to an existing dataset

If your dataset is stored in a directory `dataset.bin`, then you can add `200` outliers with the following command:

    ./run InjectOutliers -i dataset.bin/ --output dataset.csv --outliers 200 --factor 10 --num-partitions 1

To get a description of the available options, execute `./run InjectOutliers --help`.
