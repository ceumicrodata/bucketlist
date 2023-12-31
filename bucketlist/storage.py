import json
import hashlib
import os
from collections import defaultdict

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

# With help from ChatGPT4, https://chat.openai.com/share/02a14a64-01f8-4547-935c-9f5e2d6b5eac

# DiskDict class
class DiskDict:
    def __init__(self, root_dir='serialized_data'):
        self.root_dir = root_dir

    def _get_hash_and_dirs(self, key):
        hash_object = hashlib.sha256(str(key).encode())
        hex_dig = hash_object.hexdigest()
        first_folder = hex_dig[:2]
        second_folder = hex_dig[2:4]
        return hex_dig, first_folder, second_folder
    
    def get_file_path(self, key):
        hex_dig, first_folder, second_folder = self._get_hash_and_dirs(key)
        return os.path.join(self.root_dir, first_folder, second_folder, f"{hex_dig}.jsonl")

    def put(self, key, value_list):
        hex_dig, first_folder, second_folder = self._get_hash_and_dirs(key)
        first_folder_path = os.path.join(self.root_dir, first_folder)
        second_folder_path = os.path.join(first_folder_path, second_folder)
        os.makedirs(second_folder_path, exist_ok=True)
        filename = os.path.join(second_folder_path, f"{hex_dig}.jsonl")
        with open(filename, 'wt') as f:
            for value in value_list:
                jsonline_str = json.dumps(value)
                f.write(jsonline_str + '\n')

    def get(self, key):
        hex_dig, first_folder, second_folder = self._get_hash_and_dirs(key)
        filename = os.path.join(self.root_dir, first_folder, second_folder, f"{hex_dig}.jsonl")
        if not os.path.exists(filename):
            return None
        with open(filename, 'rt') as f:
            return [json.loads(line.strip()) for line in f]

    def __contains__(self, key):
        hex_dig, first_folder, second_folder = self._get_hash_and_dirs(key)
        filename = os.path.join(self.root_dir, first_folder, second_folder, f"{hex_dig}.jsonl")
        return os.path.exists(filename)

# JSONlStorage class
class JSONLStorage(AbstractStorage):
    def __init__(self, root_dir='serialized_data'):
        self._disk_dict = DiskDict(root_dir)
        self.open()

    def __contains__(self, key):
        return key in self._disk_dict

    def __len__(self):
        return sum(len(files) for _, _, files in os.walk(self._disk_dict.root_dir))

    def get(self, key):
        return self._disk_dict.get(key)

    def put(self, key, value):
        existing_values = self._disk_dict.get(key) or []
        existing_values.append(value)
        self._disk_dict.put(key, existing_values)

    def put_all(self, key, values):
        self._disk_dict.put(key, values)

    def open(self):
        os.makedirs(self._disk_dict.root_dir, exist_ok=True)

    def close(self):
        pass

class CachedStorage(AbstractStorage):
    def __init__(self, storage: AbstractStorage, cache_size=100000):
        self._storage = storage
        self.cache = {}
        self.cache_size = cache_size
        self.cache_dirty = set()  # Keep track of keys that need to be written to disk

    def __contains__(self, key):
        return key in self.cache or self._storage.__contains__(key) 
    
    def _check_cache_limit(self):
        if len(self.cache) > self.cache_size:
            oldest_key = list(self.cache.keys())[0]
            if oldest_key in self.cache_dirty:
                super().put(oldest_key, self.cache[oldest_key])
            self.cache.pop(oldest_key)
            self.cache_dirty.discard(oldest_key)

    def open(self):
        return self._storage.open()

    def get(self, key):
        if key in self.cache:
            return self.cache[key]

        value = self._storage.get(key)
        if value is not None:
            self._check_cache_limit()
            self.cache[key] = value
        return value

    def put(self, key, value):
        # Mark this key as dirty (modified)
        self.cache_dirty.add(key)

        # Update the cache
        if key in self.cache:
            self.cache[key].append(value)
        else:
            existing_values = self._storage.get(key) or []
            existing_values.append(value)
            self._check_cache_limit()
            self.cache[key] = existing_values

    def close(self):
        # Write only the "dirty" keys to disk
        for key in self.cache_dirty:
            self._storage.put_all(key, self.cache[key])
        self._storage.close()
        self.cache_dirty.clear()
