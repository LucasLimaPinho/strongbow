# strongbow

An [Apache Spark](https://spark.apache.org/) comprehensive guide with PySpark.

#### Introduction

* MapReduce: distribituted processing framework. Allowed us to divide the data processing job in smaller tasks and utilize the cluster of computers to finish the individual tasks independently and combine the results after;
* Ingest Layer: bring data in it's raw format to the Data Lake; Tools: Kafka, AWS Glue, Talend, etc.
* Store Layer: HDFS, Amazon S3, Azure Blob, GCP Cloud;
* Process Layer: Initial data quality check, transforming and preparing data, correlating, aggregating, applying machine learning models; There is a division into
  - Data Processing Layer: **Apache Spark falls here**
  - Data Orchestration Layer: Managing resources of the cluster of distributed computation; **Hadoop YARN, Kubernetes, Apache Mesos**
* Consume Layer: Data Scientists, REST interfaces, file download, JDBC/ODBC, Search Engines;
* **Apache Spark**: 
  * Hadoop YARN Cluster Manager is the most used Resource Manager for the Spark Engine because it was first build with HDFS. But now we also have Kubernetes and Apache Mesos;
  * We also have Spark stand-alone resource managers;
  * Apache Spark does not offer Cluster Management and Storage Management. All you can do is run your data processing and this is managed by the Spark Compute Engine;
  * **Spark Engine**:
    * **Spark Engine** is responsible for breaking your data processing job in smaller tasks and schedule this tasks in the cluster for parallelization;
    * **Spark Engine** is responsible for managing and monitoring thoses tasks;
    * **Spark Engine** is responsible for providing fault-tolerance when a job fails;
    * **Spark Engine** is responsible for interacting with the Cluster Manager and Storage Manager;
  * **Spark Core APIs**: Java, Scala, Python, R
  * **Layers Above the Spark Core APIs**: Spark SQL Data Frames, Streaming, Mllib (Machine Learning), GraphX (Graph Computation)

#### Execution Methods

* Interactive Clients: spark-shell, notebooks

* Submit Job: spark-submit; submit your spark jobs through the cluster;

#### How your Spark application runs?

* Spark applies a **master-slave architecture** for every application that it submits to the cluster; When we submit a application to the cluster, Spark will create a **MASTER PROCESS** for your application. This master process will create a bunch of slaves to distribute the work and do the individual tasks;
* In Spark terminology, the MASTER is a DRIVER and the slaves are the EXECUTORS; We are not talking about the cluster. The cluster itself might have a master node and a their slave nodes - we are talking about the application perspective;
* The Spark Engine is going to ask for a **container** in the underlying Cluster Manager to start the Driver Process. Once it started, the Driver will ask for more containers to start the Executors process. This happens for **each application**
* **How Spark run on a local machine?** 

~~~python
[SPARK_APP_CONFIGS]

# Spark application running locally with 03 multiple threads
spark.master=local[3]

~~~


