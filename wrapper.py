from pyiceberg.catalog import Catalog, _ENV_CONFIG, merge_config, CatalogType, _import_catalog, TYPE, PY_CATALOG_IMPL, logger, infer_catalog_type, AVAILABLE_CATALOGS
from typing import *
from pyiceberg.cache import LRUCache
from pyiceberg.typedef import Properties
from pyiceberg.catalog.glue import GlueCatalog


class GlueCatalogCache(GlueCatalog):
    def __init__(self, name: str, cache: LRUCache, properties: Properties):
        super().__init__(name, **properties)
        self.cache = cache

    def create_table(self, identifier, schema, location = None, partition_spec = ..., sort_order = ..., properties = ...):
        table = super().create_table(identifier, schema, location, partition_spec, sort_order, properties)
        return table
    
    def load_table(self, identifier):
        return super().load_table(identifier)

def load_cacheglue(name: str, cache: LRUCache, conf: Properties) -> Catalog:
    return GlueCatalogCache(name, cache, **conf)
