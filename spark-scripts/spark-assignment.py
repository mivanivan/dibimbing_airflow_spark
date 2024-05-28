import pyspark
import os
import argparse

from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Load environment variables from .env file
dotenv_path = Path(r"C:\Users\ASUS TUF\Documents\9. Materi Data Scientist\Dibimbing.id\Live Code\11. Batch Processing with PySpark\dibimbing_spark_airflow\.env")
load_dotenv(dotenv_path=dotenv_path)

postgres_port = os.getenv('POSTGRES_PORT')
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Ivan')

    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

retail_df = spark.read.jdbc(
    jdbc_url,
    'public.retail',
    properties=jdbc_properties
)

retail_df.show(5)

# Melakukan aggregasi untuk mengetahui besarnya transaksi per negara untuk mengetahui omset negara terbesar
agg_df = retail_df.withColumn("total_value", col("unit price") * col("quantity")) \
                  .groupBy("country") \
                  .agg(spark_sum("total_value").alias("total_value_sum"))

# Membuat Tabel tersendiri untuk melihat hasil agregasi dimana setiap hari akan di update oleh data terbaru
agg_df.write.jdbc(
    url=jdbc_url,
    table='public.aggregation_country',
    mode='overwrite', 
    properties=jdbc_properties
)

agg_df.show(5)