from pyspark import SparkConf, SparkContext
from utilities.helper import get_data_file_path

file_path = "book.txt"

conf = SparkConf().setMaster("local[*]").setAppName("FlatmapRDD")
sc = SparkContext(conf=conf)

lines = sc.textFile(get_data_file_path(file_path))
# words = lines.map(lambda x: x.split())
# print(words.collect())
# it will create list of lists
# so map is just splitting every line into list of words. So, in the end we would have same number of
# lists as original lines number count.
#[
#   ['Self-Employment:', 'Building', 'an', 'Internet', 'Business', 'of', 'One'],
#   ['Achieving', 'Financial', 'and', 'Personal', 'Freedom', 'through', 'a', 'Lifestyle', 'Technology', 'Business'],
#   ['By', 'Frank', 'Kane'], [], [], [],
#   ['Copyright', '�', '2015', 'Frank', 'Kane.']
# ]

words = lines.flatMap(lambda x: x.split())
# print(words.collect())
# create a list of words only ...so it kinda flatens the map
# map-returned list of list of words
# flatmap returned list of words

# ['Self-Employment:', 'Building', 'an', 'Internet', 'Business', 'of', 'One', 'Achieving',
#  'Financial', 'and', 'Personal', 'Freedom', 'through', 'a', 'Lifestyle', 'Technology'
# 'Business', 'By', 'Frank', 'Kane', 'Copyright', '�', '2015', 'Frank', 'Kane']

word_count_dict = words.countByValue()


# print(word_count_dict)
# returns a dictionary : key = words
# value = number of time that word appears in dataset
# {'Self-Employment:': 1,'Building': 5, 'an': 172, 'Internet': 13, 'Business': 19, 'of': 941,
#        'One': 12, 'Achieving': 1, 'Financial': 3,'and': 901, 'Personal': 3, 'Freedom': 7,
#        'through': 55, 'a': 1148, 'Lifestyle': 5, 'Technology': 2, 'By': 9,
#        'Frank': 10, 'Kane': 7, 'Copyright': 1, '�': 174, '2015': 3, 'Kane.': 1}

for word, count in word_count_dict.items():
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print(clean_word, count)

# b'Self-Employment:' 1
# b'Building' 5
# b'an' 172
# b'Internet' 13
# b'Business' 19
# b'of' 941
# b'One' 12
# b'Achieving' 1
# b'Financial' 3
# b'and' 901
# b'Personal' 3
# b'Freedom' 7
# b'through' 55
# b'a' 1148
# b'Lifestyle' 5
# b'Technology' 2
# b'By' 9
# b'Frank' 10
# b'Kane' 7


