import unittest
import shutil
from bucketlist import InMemoryStorage, JSONlStorage, CachedJSONlStorage

# Assuming DiskDict, JSONlStorage, CachedJSONlStorage, and InMemoryStorage classes are already defined

class TestInMemoryStorage(unittest.TestCase):
    
    def setUp(self):
        self.storage = InMemoryStorage()
    
    def test_put_get(self):
        self.storage.put("key1", {"x": 1})
        self.assertEqual(self.storage.get("key1"), [{"x": 1}])
        
    def test_contains(self):
        self.storage.put("key1", {"x": 1})
        self.assertTrue("key1" in self.storage)
        
    def test_len(self):
        self.storage.put("key1", {"x": 1})
        self.storage.put("key2", {"y": 2})
        self.assertEqual(len(self.storage), 2)


class TestJSONlStorage(unittest.TestCase):
    
    def setUp(self):
        self.storage = JSONlStorage(root_dir='test_JSONlStorage')
    
    def tearDown(self):
        shutil.rmtree('test_JSONlStorage')
        
    def test_put_get(self):
        self.storage.put("key1", {"x": 1})
        self.assertEqual(self.storage.get("key1"), [{"x": 1}])
        
    def test_contains(self):
        self.storage.put("key1", {"x": 1})
        self.assertTrue("key1" in self.storage)
        
    def test_len(self):
        self.storage.put("key1", {"x": 1})
        self.storage.put("key2", {"y": 2})
        self.assertEqual(len(self.storage), 2)


class TestCachedJSONlStorage(unittest.TestCase):
    
    def setUp(self):
        self.storage = CachedJSONlStorage(root_dir='test_CachedJSONlStorage')
    
    def tearDown(self):
        shutil.rmtree('test_CachedJSONlStorage')
        
    def test_put_get(self):
        self.storage.put("key1", {"x": 1})
        self.assertEqual(self.storage.get("key1"), [{"x": 1}])
        
    def test_contains(self):
        self.storage.put("key1", {"x": 1})
        self.assertTrue("key1" in self.storage)
        
    def test_len(self):
        self.storage.put("key1", {"x": 1})
        self.storage.put("key2", {"y": 2})
        self.assertEqual(len(self.storage), 2)
        
    def test_cache(self):
        self.storage.put("key1", {"x": 1})
        self.storage.put("key1", {"x": 2})
        self.storage.close()
        
        new_storage = CachedJSONlStorage(root_dir='test_CachedJSONlStorage')
        self.assertEqual(new_storage.get("key1"), [{"x": 1}, {"x": 2}])


if __name__ == '__main__':
    unittest.main()
