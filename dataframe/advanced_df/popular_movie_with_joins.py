from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from utilities.helper import get_data_file_path
import time
start_time = time.time()
spark = SparkSession.builder.master("local[*]").appName("PopularMovieWithJoinsDF").getOrCreate()
file_path = "ml-100k/u.data"
movie_name_file_path = "ml-100k/u.ITEM"

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

movie_name_schema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieName", StringType(), True)
])
# print(get_data_file_path(file_path))
movies_df = spark.read.format("csv") \
    .schema(schema) \
    .option("sep", "\t") \
    .option("path", get_data_file_path(file_path)) \
    .load()

# getting the movies which appeared most
top_movies_df = movies_df.groupBy("movieID").count().sort(f.col("count").desc())  # sorting works
# top_movies_df = movies_df.groupBy("movieID").count().sort(movies_df.count.desc()) - sorting doesn't work
# top_movies_df = movies_df.groupBy("movieID").count().sort(f.desc("count")) - sorts works

movie_name_df = spark.read.format("csv") \
    .schema(schema=movie_name_schema) \
    .option("sep", "|") \
    .option("path", get_data_file_path(movie_name_file_path)) \
    .load()

joined_df = top_movies_df.join(movie_name_df,
                               top_movies_df.movieID == movie_name_df.movieID,
                               "left")\
    .select(top_movies_df.movieID, movie_name_df.movieName, "count")

joined_df.show()
# top_movies_df.show(10)
print( f"time elapsed: {str(time.time()-start_time)}")


spark.stop()
