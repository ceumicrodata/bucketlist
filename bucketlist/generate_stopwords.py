import string
import csv
import sys
from unidecode import unidecode
from collections import Counter

def tokenize(row):
	return unidecode(row.translate(str.maketrans('', '', string.punctuation)).lower()).split()

if __name__ == '__main__':
	word_count = Counter()

	for line in sys.stdin.readlines():
		word_count.update(tokenize(line))

	max_num = 100
	min_count = 15
	min_len = 2
	max_len = 20
	count = 0
	for word in word_count.most_common(150):
		if (count < max_num) and (word[1] >= min_count) and (len(word[0]) >= min_len) and (len(word[0]) <= max_len):
			print(word[0])
			count += 1