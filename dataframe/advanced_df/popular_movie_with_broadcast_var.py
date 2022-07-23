from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from utilities.helper import get_data_file_path
import codecs
import time
start_time = time.time()
spark = SparkSession.builder.master("local[*]").appName("PopularMovieWithBoradcastVarDF").getOrCreate()

file_path = "ml-100k/u.data"
movie_name_file_path = "ml-100k/u.ITEM"


def load_movies_name():
    movie_names = {}
    with codecs.open(get_data_file_path(movie_name_file_path), 'r', encoding='ISO-8859-1', errors='ignore') as file:
        for line in file:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


# created broadcast variable, so this dict will be broadcast to every executors on our cluster
# name_dict = is broadcast object not the dictionary itself
name_dict = spark.sparkContext.broadcast(load_movies_name())


schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])
# print(get_data_file_path(file_path))
movies_df = spark.read.format("csv") \
    .schema(schema) \
    .option("sep", "\t") \
    .option("path", get_data_file_path(file_path)) \
    .load()

# getting the movies which appeared most
top_movies_df = movies_df.groupBy("movieID").count().sort(f.desc("count"))
# top_movies_df.show(10)


def look_up_movie_name(movie_id):
    # getting the value from broadcast variable "name_dict"
    return name_dict.value[movie_id]


# registered our functions as udf
look_up_movie_name_udf = f.udf(look_up_movie_name)
# add a movieTitle column using our new udf
movies_with_name_df = top_movies_df.withColumn("movieTitle", look_up_movie_name_udf(f.col("movieID")))

movies_with_name_df.show()
print( f"time elapsed: {str(time.time()-start_time)}")


spark.stop()
