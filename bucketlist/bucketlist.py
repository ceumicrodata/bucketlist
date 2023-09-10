from collections import defaultdict
import uuid

# meta functions
def missing_or_compare(func, missing_penalty=0.15):
    def inner_func(x, y):
        if all([x, y]):
            return func(x, y)
        if not any([x, y]):
            return  (1.0 - missing_penalty)**2
        return 1.0 - missing_penalty
    return inner_func

# univariate processing functions
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
    
class AbstractStorage(object):
    def get(self, key):
        raise NotImplementedError()
    
    def put(self, key, value):
        raise NotImplementedError()
    
    def __contains__(self, key):
        raise NotImplementedError()
    
    def __len__(self):
        raise NotImplementedError()
    
    def open(self):
        raise NotImplementedError()
    
    def close(self):    
        raise NotImplementedError()

class InMemoryStorage(AbstractStorage):
    def __init__(self):
        self._data = defaultdict(list)

    def __contains__(self, key):
        return key in self._data
    
    def __len__(self):
        return len(self._data)

    def get(self, key):
        return self._data.get(key)

    def put(self, key, value):
        self._data[key].append(value)

    def open(self):
        pass

    def close(self):
        pass

class TopN(object):
    def __init__(self, n=1, group_by=None):
        self.n = n
        self._data = {}
        if group_by:
            self.group_by = group_by
        else:
            # if there is no group_by, every record will get a unique group
            self.group_by = lambda x: uuid.uuid4().int

    def _add(self, key, data):
        # do not add exact duplicates
        if data not in self._data.values():
            self._data[key] = data

    def _lowest(self):
        return min((x[1] for x in self._data.values()))
    
    def _get_lowest(self):
        for key, value in self._data.items():
            if value[1] == self._lowest():
                return key

    def put(self, data, score):
        key = self.group_by(data)
        if key not in self._data:
            if len(self._data) < self.n:
                self._add(key, (data, score))
            elif score > self._lowest():
                # remove the lowest score
                lowest_index = self._get_lowest()
                del self._data[lowest_index]
                self._add(key, (data, score))
        else:
            old_score = self._data[key][1]
            if score > old_score:
                self._data[key] = (data, score)

    def get(self):
        return list(self._data.values())

    def clear(self):
        self._data = {}

class Bucket(object):
    def __init__(self, matcher, analyzer=lambda x: x, indexer=None, n=3, storage: AbstractStorage=None):
        # instantiate storage only when a bucket is created
        if storage is None:
            storage = InMemoryStorage()
        self.matcher = matcher
        self.analyzer = analyzer
        if indexer is None:
            if self.matcher.must:
                # index key is a tuple of tuples for all (key, value) pairs in "must"
                self.indexer = lambda x: [tuple([(key, x[key]) for key in self.matcher.must])]
            elif self.matcher.either:
                # there is a separate index for each (key, value) pairs in "either"
                self.indexer = lambda x: [(key, x[key]) for key in self.matcher.either]
            else:
                self.indexer = lambda x: [None]
        else:
            self.indexer = indexer
        self._storage = storage
        self.topn = TopN(n)

    def find(self, record):
        tokenized = self.analyzer(record)
        indexes = self.indexer(tokenized)
        # compare against all indexes
        self.topn.clear()
        best_score = 0.0
        for index in indexes:
            if index not in self._storage:
                break
            for item in self._storage.get(index):
                score = self.matcher.components(item, tokenized)
                if score[0]:
                    self.topn.put(item, score[0])
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
            self._storage.put(index, tokenized)

    def save(self):
        self._storage.close()

    def load(self):
        self._storage.open()

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
