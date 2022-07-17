from pyspark import SparkConf, SparkContext
from rdd import get_data_file_path

file_path_in_data_folder = "1800.csv"
conf = SparkConf().setMaster("local[*]").setAppName("MaxTemperature")
sc = SparkContext(conf=conf)

lines_rdd = sc.textFile(get_data_file_path(file_path_in_data_folder))


def parse_lines(line):
    fields = line.split(',')
    station_id = fields[0]
    reading_type = fields[2]
    temperature_value = float(fields[3]) * 0.1 * (9.0/5.0) +32.0
    return station_id, reading_type, temperature_value


parsed_data_rdd = lines_rdd.map(parse_lines)

filtered_rdd = parsed_data_rdd.filter(lambda x: 'TMAX' in x)

key_value_rdd = filtered_rdd.map(lambda x: (x[0], x[2]))

min_temp_per_station_rdd = key_value_rdd.reduceByKey(lambda x, y: max(x, y))

print(min_temp_per_station_rdd.collect())
