import requests
import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from decouple import config

spark = SparkSession \
    .builder \
    .appName("DataExtraction") \
    .getOrCreate() 

log_file_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/accesslog.txt"
log_data = spark.read.text(log_file_url)

ip_pattern = r"(?:\d{1,3}\.){3}\d{1,3}"  # Matches IP format 91.177.205.119

# Extract matching IP addresses with parallelization
ip_addresses = log_data.filter(log_data.value.rlike(ip_pattern)).select(log_data.value.alias("ip"))

# Save extracted IP addresses to a text file
ip_addresses.rdd.repartition(2).map(lambda row: row.ip).saveAsTextFile("extracted_data.txt")


