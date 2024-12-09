from pyiceberg.catalog import load_catalog
from pyiceberg.cache import LRUCache, MRUCache, NoCache

import time

import logging
logging.basicConfig(
    filename='logfile.txt',
    level=logging.INFO,
    format='%(message)s')

CACHES = {"LRU Cache": LRUCache(), "MRU Cache": MRUCache(), "no Cache": NoCache()}

glue_database_name = 'iceberg_tutorial_db'
glue_catalog_uri = 's3://pyiceberg-proj-bucket/nyc-taxi-iceberg'
catalog = load_catalog("glue", **{"type": "glue",
                                  "s3.region": "us-east-1",
                                  })

# try various types of filters with the replacement policies
FILTERS_ = ["not store_and_fwd_flag == 'N'", 'True', 'passenger_count == 7', 'False', 'trip_distance > 1 and trip_distance < 3', 'trip_distance < 3 and trip_distance > 1', 'trip_distance < 0 or trip_distance >= 0', 'trip_distance >= 0 or trip_distance < 0']

logging.info("ONE FILTER QUERY REPEATED 10 TIMES WITH EACH CACHE TYPE: \n\n")

for cache_type, cache in CACHES.items():
    for filt in FILTERS_:
        total_time = 0
        for i in range(10):
            start_time = time.perf_counter()
            final_table = catalog.load_table(
                'iceberg_tutorial_db.nyc_taxi_iceberg').scan(row_filter=filt).to_arrow(cache)
            end_time = time.perf_counter()

            total_time += end_time - start_time
        
        logging.info(f"cached retrieval for {cache_type} (filter: {filt}) took {(total_time/10):.6f} seconds")
        logging.info(f"cache size in bytes: {cache.get_cache_byte_size()} bytes")
        logging.info("\n-----------------------------------------------------------\n")

        cache.empty()

logging.info("\n\n\n\nALL FILTERS IN A ROW FOR EACH CACHE TYPE: \n\n")

for cache_type, cache in CACHES.items():
    total_time = 0
    for filt in FILTERS_:
        start_time = time.perf_counter()
        final_table = catalog.load_table(
            'iceberg_tutorial_db.nyc_taxi_iceberg').scan(row_filter=filt).to_arrow(cache)
        end_time = time.perf_counter()

        total_time += end_time - start_time
    logging.info(f"cached retrieval for {cache_type} took {(total_time/10):.6f} seconds")
    logging.info(f"cache size in bytes: {cache.get_cache_byte_size()} bytes")
    logging.info("\n-----------------------------------------------------------\n")

    cache.empty()


logging.info("\n\n\n\nDIFFERENT LIMITS FOR CACHES: \n\n")
LIMITS_ = [None, 1000000000000000000000000000, 1000000, 1000, 10, 1, 0]

for cache_type, cache in CACHES.items():
    for limit in LIMITS_:
        total_time = 0
        for i in range(10):
            start_time = time.perf_counter()
            final_table = catalog.load_table(
                'iceberg_tutorial_db.nyc_taxi_iceberg').scan(limit=limit).to_arrow(cache)
            end_time = time.perf_counter()

            total_time += end_time - start_time
        logging.info(f"cached retrieval for {cache_type} with limit {limit} took {(total_time/10):.6f} seconds")
        logging.info(f"cache size in bytes: {cache.get_cache_byte_size()} bytes")
        logging.info("\n-----------------------------------------------------------\n")

        cache.empty()



logging.info("\n\n\n\nDIFFERENT CATALOGS: \n\n")

