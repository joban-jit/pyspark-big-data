from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utilities.helper import get_data_file_path

spark = SparkSession.builder.appName("DataFrameWithUnstructuredData").getOrCreate()

filename = "Book.txt"
# +-------------------------------------------------------------------------------------------+
# |value_into_array                                                                           |
# +-------------------------------------------------------------------------------------------+
# |[Self, Employment, Building, an, Internet, Business, of, One]                              |
# |[Achieving, Financial, and, Personal, Freedom, through, a, Lifestyle, Technology, Business]|
# |[By, Frank, Kane]                                                                          |
unstruct_data = spark.read.text(get_data_file_path(filename))
text_to_array_df = unstruct_data.withColumn("value_into_array", f.split(unstruct_data.value, "\\W+")).drop("value")

# text_to_array_df.show(truncate=False)
words_df = text_to_array_df.select(f.explode("value_into_array").alias("Words"))
filtered_df = words_df.filter(words_df.Words != "")

lowercase_words_df = filtered_df.select(f.lower(filtered_df.Words).alias("Words"))
words_with_count_df = lowercase_words_df.groupby("Words").count()
words_with_count_df.sort(f.desc("count")).show()
