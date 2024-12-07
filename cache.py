from collections import OrderedDict


class LRUCache:

    def __init__(self, capacity=64):
        self.capacity = capacity

        self.cache = OrderedDict()
        self.current_size = 0

    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]

        return -1

    def put(self, file_key, file_content) -> None:

        if file_key in self.cache:
            raise TypeError(
                f'File {file_key} already exists in cache. Aborting insertion...')

        # Add the file to the cache
        self.cache[file_key] = file_content

        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def get_cache_byte_size(self):
        total_num_bytes = 0
        for file_content in self.cache.values():
            total_num_bytes += file_content.getbuffer().nbytes
        return total_num_bytes
