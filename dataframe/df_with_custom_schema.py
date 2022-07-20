from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from utilities.helper import get_data_file_path

spark = SparkSession.builder.appName("DataFrameWithCustomSchema").getOrCreate()

filename = "1800.csv"

schema = StructType(
    [
        StructField("stationID", StringType(), True),
        StructField("data", IntegerType(), True),
        StructField("reading_type", StringType(), True),
        StructField("temperature", FloatType(), True)
    ]
)

all_data_df = spark.read \
    .format("csv") \
    .schema(schema) \
    .option("path", get_data_file_path(filename)) \
    .load()

filtered_df = all_data_df.filter(all_data_df.reading_type == "TMIN")
station_temp_df = filtered_df.select(filtered_df.stationID, filtered_df.temperature)
grouped_data_df = station_temp_df.groupby(station_temp_df.stationID).min("temperature")
min_temp_by_station_id_df = grouped_data_df.withColumn(
    "min_temp",
    f.round(f.col("min(Temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2)
).drop(f.col("min(Temperature)")).sort("min_temp")
min_temp_by_station_id_df.show()

spark.stop()
