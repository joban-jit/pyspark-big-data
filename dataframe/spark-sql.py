from pyspark.sql import SparkSession
from pyspark.sql import Row
from utilities.helper import get_data_file_path

# creating a spark session instead of sparkcontext
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

file_path = "fakefriends.csv"


def mapper_fn(line):
    fields = line.split(',')
    # converting that unstructured data into structured data
    # converting rdd data into rows
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode("utf-8")),
        age=int(fields[2]),
        numFriends=int(fields[3])
    )


# here  we are dealing with unstructured data so we are still using textFile method by getting sparkcontext
# but it not necessarily needed but this example is more about using both rdd and sparksql/dataframe
lines_rdd = spark.sparkContext.textFile(get_data_file_path(file_path))
# to make this rdd into dataframe
# as we know dataframes are just dataset of row objects
# so we need a RDD which consists of Rows, that's what we are doing into next step
people_rdd = lines_rdd.map(mapper_fn)

# converting the rdd into dataframe
schemaPeople_df = spark.createDataFrame(people_rdd)
# we are going to cache it because we are going to run queries on it
# and we want ot keep that in memory
schemaPeople_df = schemaPeople_df.cache()
# to actual query the dataframe as a database table we need to create
# a temporary view so that's what we are doing in next step
# registering the dataframe as table and it uses the Schema which we provided while creating Rows
schemaPeople_df.createOrReplaceTempView("people_table")
# now we have a view = "people_table" which we use it as a database table

# SQL can be run over the Dataframes that have been registered as a table
teenagers_df = spark.sql("SELECT * FROM people_table WHERE age >=13 AND age <=19")

# results of sql queries are actually dataframe but also supports RDDs normal operations
for teen in teenagers_df.collect():
    print(teen)
# result:
    # Row(ID=21, name="b'Miles'", age=19, numFriends=268)
    # Row(ID=52, name="b'Beverly'", age=19, numFriends=269)
    # Row(ID=54, name="b'Brunt'", age=19, numFriends=5)
    # Row(ID=106, name="b'Beverly'", age=18, numFriends=499)
    # Row(ID=115, name="b'Dukat'", age=18, numFriends=397)

# OR

teenagers_df.show(5)
# result:
# +---+----------+---+----------+
# | ID|      name|age|numFriends|
# +---+----------+---+----------+
# | 21|  b'Miles'| 19|       268|
# | 52|b'Beverly'| 19|       269|
# | 54|  b'Brunt'| 19|         5|
# |106|b'Beverly'| 18|       499|
# |115|  b'Dukat'| 18|       397|
# +---+----------+---+----------+

# we can also use functions instead of sql queries
schemaPeople_df.groupby("age").count().orderBy("age").show(5)
# result:
# +---+-----+
# |age|count|
# +---+-----+
# | 18|    8|
# | 19|   11|
# | 20|    5|
# | 21|    8|
# | 22|    7|
# +---+-----+

# stopping the sparksession
spark.stop()
