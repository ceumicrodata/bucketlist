from collections import defaultdict
try:
    import ujson as json
except ImportError:
    import json

def bigram(text):
    nows = ''.join(text.split())
    return [''.join(z) for z in zip(nows, nows[1:])]

def ngram(text, n=3):
    text = '_' + '_'.join(text.lower().split()) + '_'
    output = list(text)
    for k in range(n-1):
        output = [i+j for i, j in zip(output, text[1+k:])]
    return set(output)

def fingerprint(lst):
    return ''.join(sorted(list(set(lst))))

def separate_stopwords(text, stopwords):
    # split a text into a tuple of non-stopwords and stopwords 
    b = [word for word in text.split() if word not in stopwords]
    s = [word for word in text.split() if word in stopwords]
    return (' '.join(b), ' '.join(s))

def remove_stopwords(text, stopwords):
    return separate_stopwords(text, stopwords)[0]

class TopN(object):
    def __init__(self, n=1):
        self.n = n
        self._data = []
        self._lowest = None

    def _add(self, data):
        # do not add exact duplicates
        if data not in self._data:
            self._data.append(data)

    def put(self, data, score, comp):
        if len(self._data) < self.n:
            self._add((data, score, comp))
            self._data.sort(key=lambda x: x[1], reverse=True)
            self._lowest = self._data[-1][1]
        elif score > self._lowest:
            del self._data[-1]
            self._add((data, score, comp))
            self._data.sort(key=lambda x: x[1], reverse=True)
            self._lowest = self._data[-1][1]

    def get(self):
        return self._data

    def clear(self):
        self._data = []
        self._lowest = None

class Bucket(object):
    def __init__(self, matcher, analyzer=lambda x: x, indexer=None, n=3):
        self.matcher = matcher
        self.analyzer = analyzer
        if indexer is None:
            if self.matcher.must:
                # index key is a tuple of tuples for all (key, value) pairs in "must"
                self.indexer = lambda x: [tuple([(key, x[key]) for key in self.matcher.must])]
            elif self.matcher.either:
                # there is a separate index for each (key, value) pairs in "must"
                self.indexer = lambda x: [(key, x[key]) for key in self.matcher.either]
            else:
                self.indexer = lambda x: [None]
        else:
            self.indexer = indexer
        self._index = defaultdict(list)
        self.topn = TopN(n)

    def find(self, record):
        tokenized = self.analyzer(record)
        indexes = self.indexer(tokenized)
        # compare against all indexes
        self.topn.clear()
        best_score = 0.0
        for index in indexes:
            for item in self._index[index]:
                score = self.matcher.components(item, tokenized)
                if score[0]:
                    self.topn.put(item, score[0], score[1])
                    best_score = max(score[0], best_score)
                if best_score > 0.9999:
                    # if perfect score is reached, no point in going further
                    break
            if best_score > 0.9999:
                # if perfect score is reached, no point in going further
                break
        return self.topn.get()

    def put(self, record):
        tokenized = self.analyzer(record)
        indexes = self.indexer(tokenized)
        # put in all indexes
        for index in indexes:
            self._index[index].append(tokenized)

    def save(self, where):
        json.dump(self._index, where)

    def load(self, where):
        self._index = json.load(where)

class Matcher(object):
    def __init__(self, must=[], either=[], sequential=[], should=[], stone_geary=0.25):
        '''
        "must" is a list of dictionary keys, *all* of which must match exactly.

        "either" is a list of dictionary keys, *any* of which must match exactly.

        "sequential" is a list of 2-way tuples of (key, callable). If the 
        callable(row1[key], row2[key]) returns False, the matching stops with a score of zero.

        "should" is a list of tuples, with first element giving a key, second element a 
        callable returning a similarity between 0.0 and 1.0. the optional third element 
        gives a non-negative weight

        "stone_geary" sets a minimum score for keys with similarity = 0.0. This can ensure
        that the product of scores does not become zero if one key does not match. 
        '''
        self.stone_geary = stone_geary
        self.must = must
        self.either = either
        self.should = []
        for e in sequential:
            if not callable(e[1]):
                raise TypeError('Second element of tuple should be a callable(x, y).')
        self.sequential = sequential
        for item in should:
            if len(item) < 3:
                weight = 1.0
            else:
                weight = item[2]
            self.should.append(dict(key=item[0], similarity=item[1], weight=weight))

    def components(self, record1, record2):
        if self.must and not all((record1[key] == record2[key] for key in self.must)):
            return (0.0, {})
        if self.either and not any((record1[key] == record2[key] for key in self.either)):
            return (0.0, {})
        for key_tuple in self.sequential:
            key, scoring = key_tuple
            if not scoring(record1[key], record2[key]):
                return (0.0, {})
        comp = {}
        i = 0
        score = 1.0
        weight = 0.0
        for key in self.should:
            comp[key['key']+"{0}".format(i)] = key['similarity'](record1[key['key']], record2[key['key']])
            score = score * (self.stone_geary + (1 - self.stone_geary) *
                (comp[key['key']+"{0}".format(i)])) ** key['weight']
            weight += key['weight']
            i += 1
        return (score, comp)

if __name__ == '__main__':
    record1 = dict(zip='02134', name='alpha co')
    record2 = dict(zip='08540', name='aleph ltd')
    record3 = dict(zip='02134', name='beta inc')

    record4 = dict(zip='02134', name='alpha company')
    record5 = dict(zip='02134', name='beta')

    from Levenshtein import ratio, jaro_winkler

    matcher = Matcher(must=[], 
        either=['bigram', 'zip'],
        should=[
        ('name', jaro_winkler),
        ('zip', ratio, 0.5)])

    def tokenizer(row):
        row['bigram'] = row['name'][0:2].upper()
        return row

    bucket = Bucket(matcher, tokenizer)

    for i in [record1, record2, record3]:
        bucket.put(i)

    print(bucket.find(record4))
    print(bucket.find(record5))
