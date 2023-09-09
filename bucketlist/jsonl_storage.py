from bucketlist import AbstractStorage
import json
import hashlib
import os
from collections import defaultdict

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

    def put(self, key, value_list):
        hex_dig, first_folder, second_folder = self._get_hash_and_dirs(key)
        first_folder_path = os.path.join(self.root_dir, first_folder)
        second_folder_path = os.path.join(first_folder_path, second_folder)
        os.makedirs(second_folder_path, exist_ok=True)
        filename = os.path.join(second_folder_path, f"{hex_dig}.jsonl")
        with open(filename, 'a') as f:
            jsonline_str = json.dumps(value_list)
            f.write(jsonline_str + '\n')

    def get(self, key):
        hex_dig, first_folder, second_folder = self._get_hash_and_dirs(key)
        filename = os.path.join(self.root_dir, first_folder, second_folder, f"{hex_dig}.jsonl")
        if not os.path.exists(filename):
            return None
        with open(filename, 'r') as f:
            return [json.loads(line.strip()) for line in f]

    def key_exists(self, key):
        hex_dig, first_folder, second_folder = self._get_hash_and_dirs(key)
        filename = os.path.join(self.root_dir, first_folder, second_folder, f"{hex_dig}.jsonl")
        return os.path.exists(filename)

# JSONlStorage class
class JSONlStorage(AbstractStorage):
    def __init__(self, root_dir='serialized_data'):
        self._disk_dict = DiskDict(root_dir)
        self.open()

    def __contains__(self, key):
        return self._disk_dict.key_exists(key)

    def __len__(self):
        return sum(len(files) for _, _, files in os.walk(self._disk_dict.root_dir))

    def get(self, key):
        return self._disk_dict.get(key)

    def put(self, key, value):
        existing_values = self._disk_dict.get(key) or []
        existing_values.append(value)
        self._disk_dict.put(key, existing_values)

    def open(self):
        os.makedirs(self._disk_dict.root_dir, exist_ok=True)

    def close(self):
        pass

class CachedJSONlStorage(JSONlStorage):
    def __init__(self, root_dir='serialized_data', cache_size=100000):
        super().__init__(root_dir)
        self.cache = {}
        self.cache_size = cache_size
        self.cache_dirty = set()  # Keep track of keys that need to be written to disk

    def _check_cache_limit(self):
        if len(self.cache) > self.cache_size:
            oldest_key = list(self.cache.keys())[0]
            if oldest_key in self.cache_dirty:
                super().put(oldest_key, self.cache[oldest_key])
            self.cache.pop(oldest_key)
            self.cache_dirty.discard(oldest_key)

    def get(self, key):
        if key in self.cache:
            return self.cache[key]

        value = super().get(key)
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
            existing_values = super().get(key) or []
            existing_values.append(value)
            self._check_cache_limit()
            self.cache[key] = existing_values

    def close(self):
        # Write only the "dirty" keys to disk
        for key in self.cache_dirty:
            super().put(key, self.cache[key])
        super().close()
        self.cache_dirty.clear()

# Example usage
storage = JSONlStorage()
storage.put("key1", {"x": 1})
print(storage.get("key1"))  # Should read from cache after the first time
storage.close()  # Write cache to disk
