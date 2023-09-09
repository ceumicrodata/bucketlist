from bucketlist import AbstractStorage
import json
import hashlib
import os

# With help from ChatGPT4, https://chat.openai.com/share/c3e011ef-9a34-4b34-bd88-00855acc1e37

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