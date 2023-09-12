import unittest as ut 
import bucketlist as search

class TestMissingOrCompare(ut.TestCase):

    def test_both_non_empty(self):
        func = lambda x, y: float(x == y)  # 1.0 if x == y, else 0.0
        result = search.missing_or_compare(func)("abc", "abc")
        self.assertAlmostEqual(result, 1.0)

        result = search.missing_or_compare(func)("abc", "def")
        self.assertAlmostEqual(result, 0.0)

    def test_both_empty(self):
        func = lambda x, y: float(x == y)
        result = search.missing_or_compare(func)("", "")
        self.assertAlmostEqual(result, (1.0 - 0.15)**2)

    def test_one_empty(self):
        func = lambda x, y: float(x == y)
        result = search.missing_or_compare(func)("", "abc")
        self.assertAlmostEqual(result, 1.0 - 0.15)

        result = search.missing_or_compare(func)("abc", "")
        self.assertAlmostEqual(result, 1.0 - 0.15)

    def test_custom_penalty(self):
        func = lambda x, y: float(x == y)
        result = search.missing_or_compare(func, missing_penalty=0.2)("", "")
        self.assertAlmostEqual(result, (1.0 - 0.2)**2)

        result = search.missing_or_compare(func, missing_penalty=0.2)("abc", "")
        self.assertAlmostEqual(result, 1.0 - 0.2)

    def test_with_different_func(self):
        func = lambda x, y: 0.5  # Always returns 0.5
        result = search.missing_or_compare(func)("abc", "abc")
        self.assertAlmostEqual(result, 0.5)

        result = search.missing_or_compare(func)("abc", "def")
        self.assertAlmostEqual(result, 0.5)

class TestTopN(ut.TestCase):
	def setUp(self):
		self.topn = search.TopN(3)

	def test_topn_is_empty(self):
		self.assertEqual(len(self.topn.get()), 0)

	def test_topn_is_not_empty(self):
		self.topn.put('a', 1.0)
		self.assertEqual(len(self.topn.get()), 1)

	def test_topn_with_few_records(self):
		self.topn.put('a', 1.0)
		self.topn.put('b', 0.5)
		self.assertEqual(len(self.topn.get()), 2)

	def test_topn_with_many_records(self):
		self.topn.put('a', 1.0)
		self.topn.put('b', 0.5)
		self.topn.put('c', 0.2)
		self.topn.put('d', 0.1)
		self.assertEqual(len(self.topn.get()), 3)
		self.assertSetEqual(set(self.topn.get()), set([('a', 1.0), ('b', 0.5), ('c', 0.2)]))

	def test_group_collision(self):
		group_topn = search.TopN(3, group_by=lambda x: x[0])
		group_topn.put('apple', 0.9)
		group_topn.put('alpha', 0.8)
		self.assertEqual(len(group_topn.get()), 1)

		group_topn.put('banana', 0.9)
		self.assertEqual(len(group_topn.get()), 2)

	def test_best_by_group(self):
		group_topn = search.TopN(3, group_by=lambda x: x[0])
		group_topn.put('apple', 0.9)
		group_topn.put('alpha', 0.8)
		group_topn.put('ananas', 0.95)
		group_topn.put('banana', 0.9)
		self.assertEqual(len(group_topn.get()), 2)
		self.assertSetEqual(set(group_topn.get()), set([('ananas', 0.95), ('banana', 0.9)]))


class TestBucket(ut.TestCase):
	def setUp(self):
		self.matcher = search.Matcher(should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		self.data = [{'name': value,'letter': value[0], 'second': value[1]} for value in 'alpha beta gamma delta'.split()]

	def test_no_common_storage(self):
		bucket1 = search.Bucket(matcher=self.matcher)
		bucket2 = search.Bucket(matcher=self.matcher)
		bucket1.put(self.data[0])
		self.assertEqual(len(bucket2._storage), 0)

	def test_finds_perfect_match(self):
		# we have to instantiate a new in memory storage for each test
		bucket = search.Bucket(matcher=self.matcher, n=1)
		for row in self.data:
			bucket.put(row)
		found = bucket.find(self.data[1])
		self.assertEqual(found[0][0], self.data[1])

	def test_perfect_match_has_score_1(self):
		bucket = search.Bucket(matcher=self.matcher, n=1)
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
		# "banana" is in the same index as "beta"
		found = bucket.find({'name': 'banana'})
		self.assertEqual(len(found), 1)

	def test_must_one(self):
		matcher = search.Matcher(must=['letter'], should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		bucket = search.Bucket(matcher=matcher)
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple', 'letter': 'a'})
		self.assertEqual(len(bucket._storage), 4)

	def test_must_multiple(self):
		matcher = search.Matcher(must=['letter', 'second'], should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		bucket = search.Bucket(matcher=matcher)
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple', 'letter': 'a', 'second': 'p'})
		self.assertEqual(len(bucket._storage), 5)

	def test_either_multiple(self):
		matcher = search.Matcher(either=['letter', 'second'], should=[('name', lambda x, y: 1.0 if x==y else 0.25)])
		bucket = search.Bucket(matcher=matcher)
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple', 'letter': 'a', 'second': 'p'})
		self.assertEqual(len(bucket._storage), 8)

	def test_returns_best_from_index(self):
		bucket = search.Bucket(matcher=self.matcher, 
			indexer=lambda x: x['name'][0], n=1)
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple'})
		found = bucket.find({'name': 'apple'})
		self.assertEqual(found[0][0]['name'], 'apple')

	def test_best_by_group(self):
		bucket = search.Bucket(matcher=self.matcher, indexer=lambda x: x['name'][0], n=3, group_by=lambda x: x['name'][0])
		for row in self.data:
			bucket.put(row)
		bucket.put({'name': 'apple'})
		found = bucket.find({'name': 'apple'})
		self.assertEqual(len(found), 1)
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

class TestEither(ut.TestCase):
	def setUp(self):
		self.analyzer = lambda value : {'name': value, 'letter': value[0], 'second': value[1]}
		self.data = [self.analyzer(value) for value in 'karma korma darma fungus'.split()]
		self.matcher = search.Matcher(either=['letter', 'second'])
		
	def test_neither(self):
		test_row = self.analyzer('beta')
		for row in self.data:
			score = self.matcher.components(row, test_row)[0]
			self.assertEqual(score, 0.0)

	def test_multiple_match(self):
		test_row = self.analyzer('karma')
		self.assertEqual(
			len([row for row in self.data if self.matcher.components(row, test_row)[0] == 1.0]),
			3)


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