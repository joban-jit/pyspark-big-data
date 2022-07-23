from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utilities.helper import get_data_file_path

spark = SparkSession.builder.appName("PopularSuperheroDF").getOrCreate()
# data in this file: first element is superhero id and rest are its connections
marvel_graph_file = "Marvel+Graph.txt"
# id and name
marvel_names_file = "Marvel+Names.txt"

names_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
names_df = spark.read.format("csv") \
    .schema(names_schema) \
    .option("sep", " ") \
    .option("path", get_data_file_path(marvel_names_file)) \
    .load()
lines_df = spark.read.text(get_data_file_path(marvel_graph_file))

updated_df = lines_df.withColumn("id", f.split(f.col("value"), " ")[0]) \
    .withColumn("connections", f.size(f.split(f.col("value"), " ")) - 1)

grouped_df = updated_df.groupBy(updated_df.id).agg(f.sum("connections").alias("connections"))
most_popular = grouped_df.sort(f.col("connections").desc()).first()
print(most_popular[0])
most_popular_with_name = names_df.filter(names_df.id == most_popular[0]).select("name").first()
print("Most popular marvel character with most connections is " + most_popular_with_name[0])
