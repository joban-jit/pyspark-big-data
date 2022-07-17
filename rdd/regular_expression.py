from pyspark import SparkConf, SparkContext
from utilities.helper import get_data_file_path
import re

file_path = "book.txt"
# same example as flatmap_rdd but we are normalizing the words
# so not just splitting up the words on space,
# but first we are removing any other characters other than alphanumerics
conf = SparkConf().setMaster("local[*]").setAppName("RegularExpression")
sc = SparkContext(conf=conf)

lines = sc.textFile(get_data_file_path(file_path))


# W+ means break up based on words and reg ex engine knows on it's own how to do that and strip
# out the punctuations and other things that aren't really part of words automatically.

# re.UNICODE : we are telling it that it may have Unicode information in it.

# text.lower(): so we don't get results based on Capitalization/based on case of words
def normalize_words_using_reg_ex(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


words = lines.flatMap(normalize_words_using_reg_ex)
# word_count_dict = words.countByValue()
# to sort it better we have commented out the above line and manually counting and sorting
# counting
kv_pair_rdd = words.map(lambda x: (x, 1))
word_counts_rdd = kv_pair_rdd.reduceByKey(lambda x, y: x + y)

# sorting
flipped_pair_position_rdd = word_counts_rdd.map(lambda x, y: (y, x))
# word_counts_sorted_rdd = flipped_pair_position_rdd.sortByKey()

# collect and print


print(flipped_pair_position_rdd.collect())

# for word, count in word_count_dict.items():
#     clean_word = word.encode('ascii', 'ignore')
#     if clean_word:
#         print(clean_word, count)
