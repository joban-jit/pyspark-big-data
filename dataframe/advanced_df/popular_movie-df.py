from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from utilities.helper import get_data_file_path

spark = SparkSession.builder.master("local[*]").appName("PopularMovieDF").getOrCreate()
file_path = "ml-100k/u.data"
schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])
# print(get_data_file_path(file_path))
movies_df = spark.read.format("csv") \
    .schema(schema)\
    .option("sep", "\t")\
    .option("path", get_data_file_path(file_path))\
    .load()

# getting the movies which appeared most
top_movies_df = movies_df.groupBy("movieID").count().sort(f.desc("count"))
# top_movies_df.show(10)


spark.stop()
