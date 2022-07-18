from pyspark.sql import SparkSession
from utilities.helper import get_data_file_path

file_path = "fakefriends-header.csv"

spark = SparkSession.builder.appName("SparkSQLwithDataframe").getOrCreate()

people_df = spark.read\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .csv(get_data_file_path(file_path))

print("here is our infered schema")
people_df.printSchema()

print("Let's display the name of column")
people_df.select("name").show(5)

print("Filter out anyone over 21: ")
people_df.filter(people_df.age < 21).show(5)

print("Group by age")
people_df.groupby("age").count().show(5)

print("Make everyone 10 years older: ")

people_df.select(people_df.name, people_df.age+10).show()

spark.stop()