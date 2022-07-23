from pyspark import SparkConf, SparkContext
from utilities.helper import get_data_file_path


file_path_in_data_folder = "fakefriends.csv"
conf = SparkConf().setMaster("local[*]").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parse_lines(line):
    fields = line.split(',')
    age = int(fields[2])
    no_of_friends = int(fields[3])
    return age, no_of_friends


lines_rdd = sc.textFile(get_data_file_path(file_path_in_data_folder))

kv_rdd = lines_rdd.map(parse_lines)

added_a_counter_rdd = kv_rdd.mapValues(lambda x: (x, 1))
totals_by_age_rdd = added_a_counter_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
average_by_age = totals_by_age_rdd.mapValues(lambda x: x[0]/x[1])
results = average_by_age.collect()
for result in results:
    print(result)
