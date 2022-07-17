from pyspark import SparkConf, SparkContext
from utilities.helper import get_data_file_path

file_path = "customer-orders.csv"
conf = SparkConf().setMaster("local[*]").setAppName("RegularExpression")
sc = SparkContext(conf=conf)

lines = sc.textFile(get_data_file_path(file_path))


def parse_file(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    spending_amount = float(fields[2])
    return customer_id, spending_amount


parsed_data_rdd = lines.map(parse_file)

# key_value_rdd = parsed_data_rdd.mapValues(lambda x: (x, 1))

# print(key_value_rdd.collect())

customer_spending_rdd = parsed_data_rdd.reduceByKey(lambda x, y: x + y)

flipped_rdd = customer_spending_rdd.map(lambda kv: (kv[1], kv[0]))

sorted_rdd = flipped_rdd.sortByKey()

results = sorted_rdd.collect()
for result in results:
    print(f'{result[1]} spent {format(result[0], ".2f")}')
