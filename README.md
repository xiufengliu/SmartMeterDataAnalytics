Smart Meter Data Analytics
============================================

**1. Cluster-based Smart Meter Data Generator**

This is the cluster-based smart meter data generator that can generate large-scale smart meter time series in parallel. For the standalone version that runs on a single server, please refer to [Data Generator (Standalone Version)](https://github.com/xiufengliu/DataGenerator-Standalone-Version). 

This data generator is implemented in Spark. It generates data with a real-world residential electricity consumption as the seed. For the design and the implementation details, please refer to [this document](docs/DataGenerator.pdf).

The example of running the data generator:


```sh
$ spark-submit --class ca.uwaterloo.iss4e.Driver --master yarn-client --driver-memory 4g --num-executors 30 --executor-cores 1 --executor-memory 5g SmartMeterAnalytics-1.0-SNAPSHOT.jar DataGenerator
```

**2. Study variability of Daily Consumption Patterns**

We do the K-SC clustering on the daily consumption patterns using the following data sets:

* _Residential electricty consumption data_
   * ESSEX data: The consumption data from Ontario, Canada
   * Dataport open data: 
   * Australia open data
 
* _Residential water consumption data_
 
To run the clustering algorithms, first you have to run the data preparation program to prepare the data according to the days

##### Experimental Results

You could view the experimental results at [here](http://nbviewer.jupyter.org/github/xiufengliu/SmartMeterDataAnalytics/blob/master/experiments/clustering/Figures.ipynb).