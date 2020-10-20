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
  * Python logging is not integrated with Spark
  * Create a Log4J configuration file
  * Configure Spark JVM to pick up the Log4j configuration file
  * Create a python class to get Spark's Log4j instance and use it

~~~python

# Important point about this variables
# Your log file is also distributed as your processing is also distributed.
# How are you going to collect them??
# You are going to rely on your Cluster Manager to collect the logs and maintain in one predefined place
# Fixed location on each machine - we define here with these variables
# /var/log/log4j
log4j.appender.file.File=${spark.yarn.app.container.log.dir}/${logfile.name}.log

~~~

* Setting Spark JVM Parameters: every Spark application has a SPARK_HOME environment variable
  * SPARK_HOME/conf/spark-defaults.conf
  * Spark will use the values in SPARK_HOME/conf/spark-defaults.conf to determine the variables to log4j
  * Make to sure to add this lines to SPARK_HOME/conf/spark-defaults.conf so that Cluster Manager can collect our logs from a fixed folder.
  
~~~conf

spark.driver.extraJavaOptions 	   -Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=spark-training

~~~

#### Creating Spark Session

* Every Spark application will create a Driver, even when configured as --master local[1] - than the driver has to do everything;
* Driver is going to start the executors to do most of the work;
* So the first thing in a Spark application is to create a **Spark Session Object which is your driver**;
* When you create spark-shell, it creates for your a Spark Session available with a name "spark";

~~~python

# This is the only thing necessary to create a Spark Session.
# Every Spark Application should have ONLY ONE SPARK SESSION - Driver.
# SPARK SESSION IS A SINGLETON OBJECT

 spark = SparkSession.builder \
      .appName("Spark Training") \
      .master("local[3]") \
      .getOrCreate()
 
 # After running what has to do
 
 spark.stop()
 
~~~

You can configure Spark Session with 4 different methods:

1. Environment variables;
2. SPARK_HOME/conf/spark-defaults.conf
3. spark-submit command line options
4. SparkConf Object

* SparkContext represents the connection to a Spark Cluster
* SparkContext can be used to get SparkConf object
* SparkContext was a entrypoint to Programming Spark in older versions

#### Spark Data Frames

* Spark as Processing Framework: Read, Process, Write;
* Each column has a specific data type - it is inspired in pandas dataframe;
* We are going tor read the file as a bunch of **in Memory partitions**. We can set the number of repartitions manually;
* Your dataframe is a bunch of smaller dataframes distributed through the nodes; AT runtime, your SparkSession() object (Driver) knows how many partition are in your distributed data file system; It can create a Logical In-Memory structure.
* The Driver distributes the dataframe partitions through the Executor JVM; Each Executor JVM Core is assigned with it's own partition to work on.
* **Spark will try to assign the partitions in HDFS that are closer to the Executor JVM's that are going to execute processing to reduce bandwith in the network**;
* Spark Engine will work together with your Cluster Manager to optimze this allocation between HDFS partitions and Executors JVM's;

#### Spark Transformations and Actions

* Spark Data Frame is a immutable data structure; 
* Instructions to the driver are called Transformations; 
* GroupedBy does not have method .show(), you need to apply .count();
* Narrow Transformation versus Wide Transformations; Narrow Dependency Transformation do not depend on any other partition like the clause WHERE; Wide Dependency Transformations depends on other partitions to produce valid results. Example of **Wide Dependency Transformation** is a groupBy() transformation;
* Simpling combining the outputs from Executos JVM's in Wide Dependency Transformations will not produce a valid result - when we say simply combine we mean concatenation line under line;
* When dealing with **Wide Dependency Transformations**, Spark needs to perform **Shuffle/Sort Exchange between partitions** to try to achieve a valid result; Wide Dependency Transformations: groupBy(), orderBy(), join(), distinct();
* Lazy Evaluations? Driver creates an execution plan when we have a lot of statements; Lines of transformations are not performed individually, but yes optimzed into a Execution Plan between the Executos JVMs. Execution Plans are terminated with **ACTIONS**;
* **Actions**: Read, Write, Collect, Show;

~~~python

spark = SparkSession \
  .builder\
  .appName("strongbow")\
  .config(conf=conf)\
  .getOrCreate()

# Lazy Transformations (Narrow Dependency and Wide Dependency Transformations) - will result in a Execution Plan

survey_df = load_survey_df(spark, sys.argv[1])
filtered_df = survey_df.where("Age < 40")
selected_df = filtered_df.select("Age","Gender","Country","state")
grouped_df = selected_df.groupBy("Country")
count_df = grouped_df.count()

# ACTION - Read, Write, Collect (generates python list), Show (generates complex structure)
count_df.show()

~~~

Making it look a better code, we can do this:

~~~python

spark = SparkSession \
      .builder \
      .appName("HelloSpark") \
      .master("local[2]") \
      .getOrCreate()

if len(sys.argv) != 2:
      logger.error("Usage: HelloSpark <filename>")
      sys.exit(-1)

survey_raw_df = load_survey_df(spark, sys.argv[1])
partitioned_survey_df = survey_raw_df.repartition(2)

# partitioned_survey_df will have two partitions mannually determined by us

count_df = count_by_country(partitioned_survey_df)
count_df.show()

~~~

Notice that after we stablish that survey_raw_df should be partitioned into 2, we have a Wide Dependency Transformation inside the function count_by_country that contains a groupBy() method. It indicates that Spark will do the Shuffle/Sort Exchange Partition. We don't know how many partitions it is going to generate, but we want to be able to control this process. We can do this with configuration - **spark.sql.shuffle.partitions** in spark.conf.

~~~python

[SPARK_APP_CONFIGS]
spark.app.name = Strongbow
spark.master = local[3]
spark.sql.shuffle.partitions = 2

~~~

* **Execution Plan**: Application -> Jobs -> Stages -> Tasks; Tasks are that are assigned to Executors JVMs;
* Jobs are triggered by actions; In our example, we have 2 Jobs triggered by method csv and 1 job triggered by collect. All of the execution plan can be seen at http://localhost:4040/jobs putting a stop point with "input("Press Enter")";

#### Spark Resilient Distributed Datasets (RDD)

* Unlike Dataframes, Datasets store language native objects (Java/Scala); They don't have a row/column structure; 
* RDD's are fault-tolerant; When a executor fails, the Driver (Spark Session) will assign the RDD's that were lost to other executors that remained up;
* RDD's lack row/column schema that dataframe have;
* RDD API's are based on SparkContext (old entrypoint);
* RDD's use a lot of **map transformations: takes a lambda function and runs within a loop for each line**;

#### Spark SQL

* You can run SQL queries only in a **table or a view**;
* Spark allows you to register your dataframe as View - surveyDF.createOrReplaceTempView("survey_tbl")
* SQL has no additional significant computation cost;
