import sys
from pyspark.sql import *

if __name__ == "__main__":

    # O método builder retorna um Builder Object que permite que você configure a sua Spark Session
    # Spark Session é um SINGLETON OBJECT - só pode ter uma Spark Session ativa por aplicação Spark
    # ALguams configurações como appName e master são as mais utilizadas por quase toda aplicação


    spark = SparkSession.builder \
        .appName("Spark Training") \
        .master("local[3]") \
        .getOrCreate()

    print("Starting Hello Spark.")

