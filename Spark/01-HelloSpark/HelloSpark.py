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
    #     .select("Age","Gender","Country","state")
    # survey_under_40.show()

    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    count_df.show()

    logger.info("Finished HelloSpark")
   #spark.stop()
