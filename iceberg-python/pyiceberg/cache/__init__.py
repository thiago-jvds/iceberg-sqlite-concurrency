from collections import OrderedDict
from typing import Any
from abc import abstractmethod

class Cache:
    ''' Class for making a generic cache'''
    def __init__(self, capacity=64):
        self.capacity = capacity

        self.cache = OrderedDict()
        self.current_size = 0

    def get(self, key: str) -> Any | None:
        ''' 
        Attempts to get an item from the cache bound by a particular key.

        Args:
            key (str): The key corresponding to the item being retrieved.

        Returns:
            Any: The item bound to the cache with given key.
            None: Key wasn't found in the cache so no value found.
        '''
        if key in self.cache:
            self.cache_policy(key)
            return self.cache[key]

        return None

    def put(self, key: str, item: Any) -> None:
        ''' 
        Attempts to bind an item to the cache using a particular key.

        Args:
            key (str): The key corresponding to the item being retrieved.
            item (Any): The item being cached.

        Raises:
            TypeError: If the key is not found in the cache.
        '''
        if key in self.cache:
            raise TypeError(
                f'File {key} already exists in cache. Aborting insertion...')

        # Add the file to the cache
        self.cache[key] = item
        self.current_size += item.getbuffer().nbytes

        if len(self.cache) > self.capacity:
            item_removed = self.remove_from_cache()
            self.current_size -= item_removed.getbuffer().nbytes

    def get_cache_byte_size(self) -> int:
        ''' 
        Gets the number of bytes currently cached.

        Returns:
            int: Number of bytes cached.
        '''
        return self.current_size
    
    @abstractmethod
    def remove_from_cache(self) -> Any:
        '''
        Evicts a single item from the cache based on the policy specified.

        Returns:
            Any: The value of the item evicted.
        
        Raise:
            KeyError: If there is currently nothing in the cache.
        '''

    @abstractmethod
    def cache_policy(self, key: str) -> None:
        '''
        Moves the key, value pair in the cache based on eviction policy so 
        remove_from_cache calls remove the proper item.

        Args:
            key (str): the key value corresponding to the item we're shifting
                       in cache according to the eviction policy.
        
        Raise:
            KeyError: If the key doesn't exist in the cache.
        '''


class NoCache(Cache):
    ''' Fake cache that allows program to fuction as if no cache is involved '''
    def get(self, key):
        return None
    
    def put(self, file_key, file_content):
        pass

    def remove_from_cache(self):
        pass

    def cache_policy(self, key):
        pass


class LRUCache(Cache):
    ''' Cache that implements least recently used policy '''
    def remove_from_cache(self) -> Any:
        return self.cache.popitem(last=False)[1]
    
    def cache_policy(self, key: str) -> None:
        self.cache.move_to_end(key)


class MRUCache(Cache):
    ''' Cache that implements most recently used policy '''
    def remove_from_cache(self) -> Any:
        return self.cache.popitem()[1]
    
    def cache_policy(self, key: str) -> None:
        self.cache.move_to_end(key)
