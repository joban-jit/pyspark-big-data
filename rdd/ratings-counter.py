from pyspark import SparkConf, SparkContext
import collections
from rdd.utilities.helper import get_data_file_path

file_path_in_data_folder = "ml-100k/u.data"

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
spark = SparkContext(conf=conf)
spark.setLogLevel("ERROR")

lines = spark.textFile(get_data_file_path(file_path_in_data_folder))
# example  not actual data
# 196 242 3 881250949
# 186 302 3 891717742
# 22  377 1 878887116
# 244 51  2 886397596
# 166 346 1 886297596


ratings = lines.map(lambda x: x.split()[2])
# example  not actual data
# 3
# 3
# 1
# 2
# 1

result = ratings.countByValue()
# example  not actual data
# (3,2) 3 comes for 2 times
# (1,2) 1 comes for 2 times
# (2,1) 2 comes for 1 times


sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f'{key}, {value}')
    # actual result
    # 1, 6110
    # 2, 11370
    # 3, 27145
    # 4, 34174
    # 5, 21201

