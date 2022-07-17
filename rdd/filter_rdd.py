from pyspark import SparkConf, SparkContext
from rdd.utilities.helper import get_data_file_path

file_path_in_data_folder = "1800.csv"

conf = SparkConf().setMaster("local[*]").setAppName("MinTemperature")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) +32.0
    return station_id, entry_type, temperature


lines = sc.textFile(get_data_file_path(file_path_in_data_folder))
parsed_lines_rdd = lines.map(parse_line)
min_temps_filtered_rdd = parsed_lines_rdd.filter(lambda x: 'TMIN' in x[1])
station_temps_rdd = min_temps_filtered_rdd.map(lambda x: (x[0], x[2]))
min_temps = station_temps_rdd.reduceByKey(lambda x, y: min(x, y))
results = min_temps.collect()
for result in results:
    print(f'{result[0]} with temp: {result[1]}')
