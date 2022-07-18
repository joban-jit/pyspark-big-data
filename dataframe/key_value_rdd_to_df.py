from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from utilities.helper import get_data_file_path

file_path = "fakefriends-header.csv"
# average number of friends as per age

spark = SparkSession.builder.appName("FriendsByAgeWithDataframe").getOrCreate()

friends_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("path", get_data_file_path(file_path))\
    .load()
# Schema is: userID,name,age,friends
age_friends_df = friends_df.select(friends_df.age, friends_df.friends)

age_friends_df.groupBy(age_friends_df.age)\
    .agg(f.round(f.avg("friends"), 2).alias("friends_age"))\
    .sort("age")\
    .show()

spark.stop()





