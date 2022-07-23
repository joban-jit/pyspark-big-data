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
# to check what is the minimum connection count, is it zero, one or something else
min_connection_count = grouped_df.agg(f.min("connections")).first()[0]

# now we filter the records which have minimum connection count
min_connection_df = grouped_df.filter(f.col("connections") == min_connection_count)

min_connection_with_names_df = min_connection_df.join(names_df, "id")

min_connection_with_names_df.select("name").show()

