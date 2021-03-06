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

#### Spark Data Sources and Sinks

* You can connect directly to your external sources - preferable for **Streaming**;
* You can ingest your external data source to your data lake - preferable for **Batch Processing**;

#### Spark DataFrameReader API

* To integrate with internal filesystems in DataLakes;
  * spark.read
    * .format()
    * .option()
    * .option()
    * ...
    * .schema()
    * .load()
* Every datasource has a list of .options in spark.read();
* "mode" is a very important .option() in spark.read();
* .schema() -> 1. Explicit, 2. Implicit, 3. InferSchema;
* Some datasources such PARQUET and AVRO comes with a pretty well defined schema inside the datasource; In those cases, you don't need to specify a schema;
* Finally, you can call the .load() method;
* You basically can't rely on inferSchema, specially for dates, and should create Schema for a DataFrame;
* Parquet already comes with Schema included in the datafile - so you don't need to explicit specify; **Prefereable**
* We create Schema **based on Spark Data Types**; 

~~~python

# Schema construction with SparkDataTypes
# StructField -> Column Definition

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])
    
~~~

If the data parsed does not check the Schema defined, Spark will have its behaviour defined by the .option("mode", <>) parameter;

#### Spark DataFrame Writer API

* Allows you to write data;
  * spark.write
    * .format()
    * .option()
    * .option()
    * ...
    * .schema()
    * .partitionBy()
    * .bucketBy()
    * .save()
 * saveMode -> 1. append, 2. overwrite, 3. errorIfExists, 4. ignore
 * The most important thing: you need to control **Spark File Layout**
  1. Number of Files and File Size;
  2. Partitions and Buckets;
  3. Sorted Data
  
* Each partition is written by a Executor JVM in parallel
* You can repartition your dataframes before you write - generating more partitions;
* .partitionBy(col1,col2) is a powerful tool to brake your dataframes logically;Very helpfull to control the size of your partitions;
* Partition your data with .bucketBy(n, col1, col2) with fixed number of pre-defined buckets;
* sortBy() - used with .bucketBy() to generated sorted buckets.
* maxRecordsPerFile - number of maximum rows in the partition files. You can control the size of your partitions and the number of partitions generated (optional);

#### Spark Dataframe and Dataset Transformations

* Data Source -> Transformations -> Data Sink
* In Spark, we read the data and can create **Dataframes - Programatic Interface for your data ---> Spark Program** or **Tables - SQL interface for your data ---> SQL Program**;
* Transformations between Data Source and Data Sink


  1. Combining Dataframes;
  2. Aggregating and Sumarizing;
  3. Applying Functions and built-in transformations;
  4. Using built-in and column level functions;
  5. Creating and using UDF's
  6. Referencing Rows/Columns
  7. Creating Column expressions

**Working with dataframe rows**: Spark Dataframe is a dataset of rows - Dataframe = Dataset[Row]; Each row in the dataframe is a single record;

* You can a use a RDD + SChema to create a Dataframe
  * my_rdd = spark.SparkContext.parallelize(my_rows, 2)
  * my_df = spark.createDataFrame(my_rdd, my_schema)
  
~~~python

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("RowDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())])

    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()
    new_df = to_date_df(my_df, "M/d/y", "EventDate")
    new_df.printSchema()
    new_df.show()
~~~

#### Working with Dataframe Columns:

* They can't work outside of a transformation.
* airLinesDF.select("Origin", "Dest", "Distance").show(10) --> Simplest way to access columns inside a transformation; Accessing using **Column String**
* airLinesDF.select(column("Origin"), col("Dest"), airLinesDF.Distance).show(10) ---> acessing using **Column Objects**; All these methods are the same.
* you can use both Column String and Column Object together;
* expr() converts a String object into a Column object allowing to use .select()
* . withColumn() allows you to transform a SINGLE column without impacting the other columns in the dataframe; It takes two arguments. The first one is the columns name that you want to transform and the second one is a column_expression. We can use User Defined Functions (UDF) as the second argument. Ex. 

~~~python

# You need to register your User Defined Function in the SparkSession() object so that it can send to the Executors JVMs
# This one registers as a Dataframe UDF

parse_gender_udf = udf(parse_gender, StringType())
[logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name] # Python List Comprehension
survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"));

# Registering as a SQL function UDF
# THis one will also create an entry in the catalog
# If you want to use your function in a SQL expression, you must register like this

spark.udf.register("parse_gender_udf", parse_gender, StringType())
# Will work because right now is in the spark.catalog.
survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))

~~~

#### Spark DataFrame Aggregations

* Aggregagations: Simple Aggregations, Grouping Aggregations & Windwowing Aggregagations;
* All aggreggations in Spark are implemented as **built-in** function;
* Aggregagation functions: avg(), count(), sum(), max(), min()
* Window Aggregation functions: lead(), lag(), rank(), dense_rank(), cume_dist()
* We can use SQLite expressions using Expr;

~~~python

# Agregations in Spark Data Frame

    # Create Spark Session - SingleTon Object. Our Driver.
    # local[2] - 1 Driver & 1 Executor

    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    # Reading as a Dataframe using DataFrameReader API
    # inferSchema = true. Not necessary for Parquet and Avro files
    # .load(path,file)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    # All functions from pysparksql where imported as f
    # Return the count of distinct rows
    invoice_df.select(f.count("*").alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")
                      ).show()

    # We can Use Expr to use SQL Language Expressions

    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
          SELECT Country, InvoiceNo,
                sum(Quantity) as TotalQuantity,
                round(sum(Quantity*UnitPrice),2) as InvoiceValue
          FROM sales
          GROUP BY Country, InvoiceNo""")

    summary_sql.show()

    # .agg transformations are specially designed to take a list of aggregation functions
    
    
    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )

    summary_df.show()
    
 ~~~
 
 Other example now for Grouping Aggregations:
 
 ~~~python
 
 from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    # Writing aggregation function to use as parameter for the transformation .agg() and make code more readable
    # .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) transforms the column InvoiceDate to the date format that we want
    # So that can we extract the week of the year using .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
    # coalesce
    
    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    exSummary_df = invoice_df \
        .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupBy("Country", "WeekNumber") \
        .agg(NumInvoices, TotalQuantity, InvoiceValue)

    exSummary_df.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("output")

    exSummary_df.sort("Country", "WeekNumber").show()
    
 ~~~

Windowing Aggregations:

~~~python

#1. Identifying your partitioning columns -> In our example: the "Country"
#2. Identifying your ordering requirements; -> In our example: the "WeekNumber"
#3. Define your window start and end; In our example: The first report until current report;
# Examples: lag(), lead(), rank(), dense_rank()

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    summary_df = spark.read.parquet("data/summary.parquet")

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(-2, Window.currentRow)

    summary_df.withColumn("RunningTotal",
                         f.sum("InvoiceValue").over(running_total_window)) \
                          .show()
        
 ~~~

#### Spark Streaming With Spark 1.6

~~~python
        # To initialize a Spark Streaming program, a StreamingContext
        # object has to be created which is the main entry point of all Spark Streaming functionality.

        # sc = SparkContext(master, appName)
        # The appName parameter is a name for your application to show on the cluster UI.
        # master is a Spark, Mesos or YARN cluster URL or a special local[*]
        # string to run locally;

        # local[*] detects the number of cores in the local system;

        # The batch-interval must be set based on the latency requirements of your application
        # and available cluster resources. We can check
        # [Performance Tuning](https://spark.apache.org/docs/1.6.0/streaming-programming-guide.html#setting-the-right-batch-interval)
        # later to achieve better performance.

        # After a context is defined:
        #   1. Define the input sources by creating input DStreams;
        #   2. Define the streaming computations by applying transformation and output operations to DStreams;
        #   3. Start receiving data and processing it using streamingContext.start()
        #   4. Wait for the processing to be stopped (mannually or due to any error) using
        #       streamingContext.awaitTermination()
        #   5. The process can be manually stopped using streamingContext.stop()
        #

        # Points to remember:
        #   1. Once a context has been started, no new streaming computation can be set up or added to it
        #   2. Once a context has been stopped, it cannot be restarted;
        #   3. Only one StreamingContext can be active in JVM at the same time
        #   4. stop() on StreamingContext also stops the SparkContext. To stop only the StreamingContext
        #       set the optional parameter of stop() called stopSparkContext to false
        #   5. A SparkContext can be re-used to create multiple StreamingContexts, as long as the previous
        #       StreamingContext is stopped (without stopping the SparkContext) before the next
        #       StreamingContext is created

        ######### Discretized Streams (DStreams)

        # DStream is the basic abstraction provided by SparkStreaming. It represents a continuous stream
        # of data, either the input data stream received from source, or the processed data stream
        # generated by transforming the input stream. Internnaly, a DStream is represented
        # by a continuous series of RDDs which is Sparks abstraction of an immutable distributed dataset.
        # Each RDD in a DStream contains data from a *******certain interval********

        # Underlying RDD transformatins are computed by Spark Engine. The DStream operations hide most
        # of these details and provide the developer with a higher-level API for convenience.

        # Every input DStream (except file stream) is associated with a Receiver (Scala doc, Java doc)
        # object which receives the data from a source and stores it in Sparks memory for processing

        # Spark Streaming provides two categories of built-in streaming sources:
        #   1. Basic Sources: Sources directly available in the StreamingContext API.
        #       Examples: file systems, socket connections, and Akka actors
        #   2. Advanced Sources: Sources like Kafka, Flume, Kinesis, Twitter, etc. are available through
        #       extra utility classes. These require linking against extra dependencies
        #       [here] https://spark.apache.org/docs/1.6.0/streaming-programming-guide.html#linking

        # [KAFKA INTEGRATION GUIDE] (https://spark.apache.org/docs/1.6.0/streaming-kafka-integration.html)

        # Receiving data over the network (like Kafka, Flume, socket, etc.) requires the
        # data to be deserialized and stored in Spark

        # If the data receiving becomes a bottleneck in the system,
        # then consider parallelizing the data receiving.

        # Note that each input DStream creates a single receiver (running on a worker machine)
        # that receives a single stream of data. Receiving multiple data streams can therefore be
        # achieved by creating multiple input DStreams and configuring them to receive different
        # partitions of the data stream from the source(s).

        # For example, a single Kafka input DStream receiving
        # two topics of data can be split into two Kafka input streams, each receiving only one topic

        # This would run two receivers, allowing data to be received in parallel, thus increasing overall throughput.
        # These multiple DStreams can be unioned together to create a single DStream.

        # Then the transformations that were being applied on a single input DStream can be applied on the unified stream.
        # This is done as follows

numStreams = 5
kafkaStreams = [KafkaUtils.createStream(...) for _ in range (numStreams)]
unifiedStream = streamingContext.union(*kafkaStreams)
unifiedStream.pprint()

        # For most receivers, the received data is coalesced together into blocks of data before storing
        # inside Spark’s memory.
        # The number of blocks in each batch determines the number of tasks that will be used to process
        # the received data in a map-like transformation.

        # The number of tasks per receiver per batch will be approximately (batch interval / block interval).
        # For example, block interval of 200 ms will create 10 tasks per 2 second batches. If the number of tasks is too
        # low (that is, less than the number of cores per machine), then it will be inefficient as all available cores
        # will not be used to process the data. To increase the number of tasks for a given batch interval, reduce the
        # block interval. However, the recommended minimum value of block interval is about 50 ms, below which the task
        # launching overheads may be a problem

        ## LEVEL OF PARALLELISM IN DATA PROCESSING

        # Cluster resources can be under-utilized if the number of parallel tasks used in any stage of the computation is
        # not high enough. For example, for distributed reduce operations like reduceByKey and reduceByKeyAndWindow, the
        # default number of parallel tasks is controlled by the spark.default.parallelism configuration property. You can
        # pass the level of parallelism as an argument (see PairDStreamFunctions documentation), or set the
        # spark.default.parallelism configuration property to change the default.

        ## SETTING THE RIGHT BATCH INTERVAL

        # For a Spark Streaming application running on a cluster to be stable, the system should be able to process data
        # as fast as it is being received. In other words, batches of data should be processed as fast as they are being
        # generated. Whether this is true for an application can be found by monitoring the processing times in the
        # streaming web UI, where the batch processing time should be less than the batch interval.
        #
        # Depending on the nature of the streaming computation, the batch interval used may have significant impact
        # on the data rates that can be sustained by the application on a fixed set of cluster resources. For example,
        # let us consider the earlier WordCountNetwork example. For a particular data rate, the system may be able to
        # keep up with reporting word counts every 2 seconds (i.e., batch interval of 2 seconds),
        # but not every 500 milliseconds. So the batch interval needs to be set such that the expected data rate in
        # production can be sustained.
        #
        # A good approach to figure out the right batch size for your application is to test it with a conservative
        # batch interval (say, 5-10 seconds) and a low data rate. To verify whether the system is able to keep up with
        # the data rate, you can check the value of the end-to-end delay experienced by each processed batch
        # (either look for “Total delay” in Spark driver log4j logs, or use the StreamingListener interface).
        # If the delay is maintained to be comparable to the batch size, then system is stable.
        # Otherwise, if the delay is continuously increasing, it means that the system is unable to keep up and it
        # therefore unstable. Once you have an idea of a stable configuration, you can try increasing the data rate
        # and/or reducing the batch size. Note that a momentary increase in the delay due to temporary data rate
        # increases may be fine as long as the delay reduces back to a low value (i.e., less than batch size).

        # Points to remember:
        #
        # Topic partitions in Kafka does not correlate to partitions of RDDs generated in Spark Streaming.
        # So increasing the number of topic-specific partitions in the KafkaUtils.createStream() only increases the
        # number of threads using which topics that are consumed within a single receiver. It does not increase the
        # parallelism of Spark in processing the data. Refer to the main document for more information on that.
        #
        # Multiple Kafka input DStreams can be created with different groups and topics for parallel receiving of data
        # using multiple receivers.
        #
        # If you have enabled Write Ahead Logs with a replicated file system like HDFS, the received data is already
        # being replicated in the log. Hence, the storage level in storage level for the input stream
        # to StorageLevel.MEMORY_AND_DISK_SER (that is, use KafkaUtils.createStream(..., StorageLevel.MEMORY_AND_DISK_SER))

        # Approach 2: Direct Approach (No Receivers)
        # This new receiver-less “direct” approach has been introduced in Spark 1.3 to ensure stronger
        # end-to-end guarantees. Instead of using receivers to receive data, this approach periodically queries
        # Kafka for the latest offsets in each topic+partition, and accordingly defines the offset ranges to process
        # in each batch. When the jobs to process the data are launched, Kafka’s simple consumer API is used to read
        # the defined ranges of offsets from Kafka (similar to read files from a file system).
        # Note that this is an experimental feature introduced in Spark 1.3 for the Scala and Java API, in Spark 1.4
        # for the Python API.

        # This approach has the following advantages over the receiver-based approach (i.e. Approach 1).
        #
        # Simplified Parallelism: No need to create multiple input Kafka streams and union them.
        # With directStream, Spark Streaming will create as many RDD partitions as there are Kafka partitions
        # to consume, which will all read data from Kafka in parallel. So there is a one-to-one mapping between Kafka
        # and RDD partitions, which is easier to understand and tune.

        # Efficiency: Achieving zero-data loss in the first approach required the data to be stored in a Write Ahead Log,
        # which further replicated the data. This is actually inefficient as the data effectively gets replicated twice
        # - once by Kafka, and a second time by the Write Ahead Log. This second approach eliminates the problem
        # as there is no receiver, and hence no need for Write Ahead Logs. As long as you have sufficient Kafka retention,
        # messages can be recovered from Kafka.

        # Exactly-once semantics: The first approach uses Kafka’s high level API to store consumed offsets in Zookeeper.
        # This is traditionally the way to consume data from Kafka. While this approach (in combination with write ahead logs)
        # can ensure zero data loss (i.e. at-least once semantics), there is a small chance some records may get consumed
        # twice under some failures. This occurs because of inconsistencies between data reliably received by Spark Streaming
        # and offsets tracked by Zookeeper. Hence, in this second approach, we use simple Kafka API that does not use Zookeeper.
        # Offsets are tracked by Spark Streaming within its checkpoints. This eliminates inconsistencies between
        # Spark Streaming and Zookeeper/Kafka, and so each record is received by Spark Streaming effectively exactly
        # once despite failures. In order to achieve exactly-once semantics for output of your results,
        # your output operation that saves the data to an external data store must be either idempotent,
        # or an atomic transaction that saves results and offsets (see Semantics of output operations in
        # the main programming guide for further information).

~~~
   
   
