import unittest as ut 
import bucketlist as search
import io

class TestBucketSave(ut.TestCase):
	def setUp(self):
		self.stream = io.BytesIO()
		self.matcher = search.Matcher(should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		self.data = [{'name': value,'letter': value[0], 'second': value[1]} for value in 'alpha beta gamma delta'.split()]

	def test_save_then_load(self):
		bucket = search.Bucket(matcher=self.matcher)
		for row in self.data:
			bucket.put(row)
		bucket.save(self.stream)
		self.stream.seek(0)
		new_bucket = search.Bucket(matcher=self.matcher)
		new_bucket.load(self.stream)
		for key in bucket._index:
			self.assertListEqual(bucket._index[key], new_bucket._index[key])

class TestBucket(ut.TestCase):
	def setUp(self):
		self.matcher = search.Matcher(should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		self.data = [{'name': value,'letter': value[0], 'second': value[1]} for value in 'alpha beta gamma delta'.split()]

	def test_finds_perfect_match(self):
		bucket = search.Bucket(matcher=self.matcher)
		for row in self.data:
			bucket.put(row)
		found = bucket.find(self.data[1])
		self.assertEqual(found[0][0], self.data[1])

	def test_perfect_match_has_score_1(self):
		bucket = search.Bucket(matcher=self.matcher)
		for row in self.data:
			bucket.put(row)
		found = bucket.find(self.data[1])
		self.assertEqual(found[0][1], 1.0)

	def test_suboptimal_matches_returned(self):
		bucket = search.Bucket(matcher=self.matcher, n=4)
		for row in self.data:
			bucket.put(row)
		found = bucket.find({'name': 'epsilon'})
		self.assertEqual(len(found), 4)

	def test_returns_after_perfect_match(self):
		bucket = search.Bucket(matcher=self.matcher, n=4)
		for row in self.data:
			bucket.put(row)
		found = bucket.find(self.data[0])
		self.assertEqual(len(found), 1)

	def test_returns_only_from_index(self):
		bucket = search.Bucket(matcher=self.matcher, 
			indexer=lambda x: x['name'][0])
		for row in self.data:
			bucket.put(row)
		found = bucket.find({'name': 'apple'})
		self.assertEqual(len(found), 1)

	def test_must_one(self):
		matcher = search.Matcher(must=['letter'], should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		bucket = search.Bucket(matcher=matcher)
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple', 'letter': 'a'})
		self.assertEqual(len(bucket._index), 4)

	def test_must_multiple(self):
		matcher = search.Matcher(must=['letter', 'second'], should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		bucket = search.Bucket(matcher=matcher)
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple', 'letter': 'a', 'second': 'p'})
		self.assertEqual(len(bucket._index), 5)

	def test_either_multiple(self):
		matcher = search.Matcher(either=['letter', 'second'], should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		bucket = search.Bucket(matcher=matcher)
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple', 'letter': 'a', 'second': 'p'})
		self.assertEqual(len(bucket._index), 8)

	def test_returns_best_from_index(self):
		bucket = search.Bucket(matcher=self.matcher, 
			indexer=lambda x: x['name'][0])
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple'})
		found = bucket.find({'name': 'apple'})
		self.assertEqual(found[0][0]['name'], 'apple')

class Test_Matcher(ut.TestCase):
	def setUp(self):
		self.data = [{'name': value,'letter': value[0], 'second': value[1]} for 
			value in 'alpha aleph beta betamax apple'.split()]

	def test_must_one_matches(self):
		matcher = search.Matcher(must=['letter'], should=[('name', lambda x, y: 1.0 if x==y else 0.0)])
		score = matcher.components(self.data[0], self.data[1])[0]
		self.assertEqual(score, 0.25)

	def test_must_one_not_match(self):
		matcher = search.Matcher(must=['letter'], should=[('name', lambda x, y: 1.0 if x==y else 0.0)])
		score = matcher.components(self.data[0], self.data[2])[0]
		self.assertEqual(score, 0.0)

	def test_must_two_matches(self):
		matcher = search.Matcher(must=['letter', 'second'], should=[('name', lambda x, y: 1.0 if x==y else 0.0)])
		score = matcher.components(self.data[0], self.data[1])[0]
		self.assertEqual(score, 0.25)

	def test_must_two_not_match(self):
		matcher = search.Matcher(must=['letter', 'second'], should=[('name', lambda x, y: 1.0 if x==y else 0.0)])
		score = matcher.components(self.data[0], self.data[4])[0]
		self.assertEqual(score, 0.0)

	def test_both_can_be_empty(self):
		matcher = search.Matcher(should=[('column_a', lambda x, y: 1.0)])
		row = dict(column_a='whatever')
		score = matcher.components(row, row)[0]
		self.assertEqual(score, 1.0)

class TestSequantialMatcher(ut.TestCase):
	def test_sequential_can_be_one(self):
		matcher = search.Matcher(sequential=[('column_a', lambda x, y: True)])
		row = dict(column_a='whatever')
		score = matcher.components(row, row)[0]
		self.assertEqual(score, 1.0)

	def test_costly_function_not_called(self):
		global cost
		cost = False
		def costly(x, y):
			global cost
			cost = True
			return 1.0
		matcher = search.Matcher(sequential=
			[('column_a', lambda x, y: False)], 
			should=[('column_b', costly)])
		row = dict(column_a='name', column_b='address')
		score = matcher.components(row, row)
		self.assertFalse(cost)

	def test_costly_function_called(self):
		global cost
		cost = False
		def costly(x, y):
			global cost
			cost = True
			return 1.0
		matcher = search.Matcher(sequential=
			[('column_a', lambda x, y: True)], 
			should=[('column_b', costly)])
		row = dict(column_a='name', column_b='address')
		score = matcher.components(row, row)
		self.assertTrue(cost)

	def test_threshold_passed(self):
		matcher = search.Matcher(sequential=
			[('column_a', lambda x, y: True),
			],
			should=[('column_a', lambda x, y: 0.20)],
			stone_geary=0.0)
		bucket = search.Bucket(matcher=matcher)
		row = dict(column_a='name', column_b='address')
		score = matcher.components(row, row)[0]
		self.assertEqual(score, 0.20)

	def test_sequential_needs_callable(self):
		with self.assertRaises(Exception):
			matcher = search.Matcher(sequential=[('column_a')])

class TestNgram(ut.TestCase):
	def setUp(self):
		self.known_cases = [
			('apple', {'_a', 'ap', 'pp', 'pl', 'le', 'e_'}),
			('pear', {'_p', 'pe', 'ea', 'ar', 'r_'})
			]

	def test_bigram_is_length_2(self):
		for case in self.known_cases:
			for bigram in search.ngram(case[0], n=2):
				self.assertEqual(len(bigram), 2)
	
	def test_bigram_known_cases(self):
		for case in self.known_cases:
			self.assertSetEqual(case[1],
				search.ngram(case[0], n=2))

	def test_bigram_leading_space(self):
		for case in self.known_cases:
			self.assertSetEqual(case[1],
				search.ngram(' '+case[0], n=2))
	
	def test_bigram_trailing_space(self):
		for case in self.known_cases:
			self.assertSetEqual(case[1],
				search.ngram(case[0]+' ', n=2))
	
	def test_bigram_uppercase(self):
		for case in self.known_cases:
			self.assertSetEqual(case[1],
				search.ngram(case[0].upper(), n=2))

if __name__ == '__main__':
	ut.main()