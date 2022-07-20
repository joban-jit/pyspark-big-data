from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utilities.helper import get_data_file_path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("DataFrameWithUnstructuredData").getOrCreate()

file_path = "customer-orders.csv"

schema = StructType(
    [
        StructField("customerID", StringType(), True),
        StructField("itemID", StringType(), True),
        StructField("amountSpend", FloatType(), True)
    ]
)

raw_df = spark.read\
    .format("csv")\
    .schema(schema)\
    .option("path", get_data_file_path(file_path))\
    .load()

custID_amt_df = raw_df.select(raw_df.customerID, raw_df.amountSpend)
final_df = custID_amt_df.groupBy(custID_amt_df.customerID).agg(f.round(f.sum("amountSpend"), 2).alias("total_spending"))
new_Df = final_df.select(final_df.customerID, final_df.total_spending).sort(f.desc("total_spending"))
new_Df.show(new_Df.count())