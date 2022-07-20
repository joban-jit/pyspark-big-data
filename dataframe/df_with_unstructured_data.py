from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utilities.helper import get_data_file_path

spark = SparkSession.builder.appName("DataFrameWithUnstructuredData").getOrCreate()

filename = "Book.txt"

unstructured_data = spark.read.text(get_data_file_path(filename))
# text_to_array_df = unstructured_data.withColumn("value_into_array",
#                                                 f.split(unstructured_data.value, "\\W+")
#                                                 )

# words_df = text_to_array_df.select(f.explode("value_into_array").alias("Words"))
# combined the above two lines into one below statement, both works
words_df = unstructured_data.select(f.explode(f.split(unstructured_data.value, "\\W+")).alias("Words"))
filtered_df = words_df.filter((words_df.Words != "") & (f.length(words_df.Words) != 1))

lowercase_words_df = filtered_df.select(f.lower(filtered_df.Words).alias("Words"))
words_with_count_df = lowercase_words_df.groupby("Words").count()
words_with_count_df.sort(f.desc("count")).show()
