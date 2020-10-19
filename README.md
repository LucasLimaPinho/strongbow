# strongbow

An [Apache Spark](https://spark.apache.org/) comprehensive guide with PySpark.

[Code](https://github.com/LearningJournal/Spark-Programming-In-Python)

Spark UI: http://localhost:4040/jobs/

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
* Spark can run the application with 5 configurations: 1. Local, 2. Hadoop YARN, 3. Kubernetes, 4. Apache Mesos & 5. Standalone
* YARN on-Premise (basically Cloudera distributions); YARN on-Cloud (Databricks, Google DataProc, etc)
* **How Spark run on a local machine?** When you use 'spark.master = local[1]' you'll have only a Driver container and no executors. Your driver is forced to do everything by himself. When you run your application with local[3], you'll have 1 driver + 2 executors;

~~~python
[SPARK_APP_CONFIGS]

# Spark application running locally with 03 multiple threads
# If you simple says local and don't put any number - it becomes a single-threaded application

spark.master=local[3]

~~~

* **How does Spark run with interactive clients?** The driver stays in the Cliente Machine while the Executors run queries in the Cluster; Good for interactive work, but not for longer runs jobs; In the Cluster Mode, everything runs in the Cluster - Driver + Executors.

* **Running Spark In Command Line - Working with PySpark Shell**:

~~~python

# --master is the parameter that tells what is going to be the Cluster Manager. By default, is local[*]
# Options for --master parameter are spark://host:port, mesos://host:port, yarn, k8s://host:port or local(Default: local[*])

--master local[3]

# --deploy-mode : whether to launch the driver program locally ("client") or on one of the worker machines inside the cluster ("cluster")
# Default: "client"

--deploy-mode "client"

# --class CLASS_NAME -> your application main class for Java / Scala applications

# --py-files PY_FILES -> Comma separated list of .zip, .egg or .py files to place on the PYTHONPATH for python apps

# --driver-memory MEM Memory for driver (e.g. 1000M, 2G); (Default: 1024M)

# --num-executors NUM -> Only relevant for YARN and Kubernetes Cluster Manager. Defines the number of executors to launch. If dynamic allocation is enabled, the initial number of executors will be at least NUM;

# Our shell command:

pyspark --master local[3] --driver-memory 2G

~~~

**YARN Client Mode using Google DataProc Cluster and Zeppelin Notebook**: 

YARN Client mode -> Driver running on client machine and executors running on the YARN cluster. Monstly used by Data Scientists and Data Analysts to make interactive explorations. 

~~~python
# Using Spark Shell through SSH in Driver allocated in Cluster in Google DataProc
# --master yarn

pyspark --master yarn --driver-memory 1G --executor-memory 500M --num-executors 2 --executor-cores 1

~~~

**YARN Cluster Mode Spark Submit using Google DataProc and Zeppelin Notebook**:

~~~python
# Need to determine that the cell is going to run pyspark interpretator. By default, is a Scala cell

%pyspark

# pi.py is a application uploaded to the cluster

spark-submit --master yarn --deploy-mode cluster pi.py

~~~

**How to create Spark Applications**

* Environment variable **PYTHONPATH** is used by Python to specify the list of directories of which modules can be imported. 
* %SPARK_HOME% should be set to the path where are the Spark binaries;
* %SPARK_HOME% should point to the same version of pyspark imported in the Python Project
* %PYSPARK_PYTHON% should point to python.exe in the correct version
* Configuring Spark Application Logs: 
  * Create a Log4J configuration file
  * Configure Spark JVM to pick up the Log4j configuration file
  * Create a python class to get Spark's Log4j instance and use it
