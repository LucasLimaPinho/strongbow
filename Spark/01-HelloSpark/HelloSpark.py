import sys
from pyspark.sql import *
from lib.logger import *
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    survey_raw_df = load_survey_df(spark, sys.argv[1])

    # Simple transformations - Showing that we have RDD

    # survey_under_40 = survey_raw_df.where("Age < 40")\
    #     .select("Age", "Gender", "Country", "state")\
    #     .groupBy("Country")

    # Estamos dividindo a partition em two
    # partitioned_survey_df deve ter duas partições

    partitioned_survey_df = survey_raw_df.repartition(2)

    # Temos internamente na função count_by_country um processo de groupBy() que é uma
    # Wide Dependency Transformation. Portanto, Spark irá executar o processo de Shuffle/Sort Exchange
    # gerando mais partitions;
    # Nós não sabemos quantas partições teremos após o Shuffle/Sort, mas queremos controlar esse comportamento
    # Como fazer isso?
    # Podemos controlar usando configuração.


    count_df = count_by_country(partitioned_survey_df)
    count_df.show()

    logger.info("Finished HelloSpark")
   #spark.stop()
