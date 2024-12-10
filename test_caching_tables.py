from pyiceberg.catalog import load_catalog
from pyiceberg.cache import LRUCache, MRUCache, NoCache

import time

import matplotlib.pyplot as plt
import numpy as np

import logging
logging.basicConfig(
    filename='logfile.txt',
    level=logging.INFO,
    format='%(message)s')


glue_database_name = 'iceberg_tutorial_db'
glue_catalog_uri = 's3://pyiceberg-proj-bucket/nyc-taxi-iceberg'
catalog = load_catalog("glue", **{"type": "glue",
                                  "s3.region": "us-east-1",
                                  })

# try various types of filters with the replacement policies
CACHES = {"LRU Cache": LRUCache(), "MRU Cache": MRUCache(), "no Cache": NoCache()}
FILTERS_ = {"not store_and_fwd_flag == 'N'": "Str Equality + Not", 'True': "Always True", 'passenger_count == 7': "Int Equality", 'False':"Always False", 'trip_distance > 1 and trip_distance < 3': "GT + LT + And", 'trip_distance < 3 and trip_distance > 1': "GT + LT + And (flipped order)", 'trip_distance < 0 or trip_distance >= 0': "GTE + LT + Or (bad ordering)", 'trip_distance >= 0 or trip_distance < 0': "GTE + LT + Or (short circuiting)"}
LIMITS_ = {None: "None", 10**12: "Infinity", 1000000: "1000000", 1000: "1000", 10: "10", 1: "1", 0: "0"}

CACHE_POSITIONS = np.arange(len(CACHES))
FILTER_POSITIONS = np.arange(len(FILTERS_))
LIMITS_POSITIONS = np.arange(len(LIMITS_))

BAR_WIDTH = 0.25
COLORS = ['blue','red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan']

def one_cache_one_filter_repeat_10(graph=False, log=True):
    if not log:
        logging.getLogger().setLevel(logging.CRITICAL) 

    logging.info("ONE FILTER QUERY REPEATED 10 TIMES WITH EACH CACHE TYPE: \n\n")

    i = 0
    all_data = {}
    for cache_type, cache in CACHES.items():
        all_data[cache_type] = []
        for filt in FILTERS_:
            total_time = 0
            for _ in range(10):
                start_time = time.perf_counter()
                catalog.load_table('iceberg_tutorial_db.nyc_taxi_iceberg').scan(row_filter=filt).to_arrow(cache)
                end_time = time.perf_counter()

                total_time += end_time - start_time
            all_data[cache_type].append(total_time/10)

            logging.info(f"{cache_type} (filter: {filt}): {(total_time/10):.6f} seconds, {cache.get_cache_byte_size()} bytes")
            
            cache.empty()
        logging.info("\n-----------------------------------------------------------\n")


    if graph:
        for i, (cache_type, runtimes) in enumerate(all_data.items()):
            plt.bar(FILTER_POSITIONS + i * BAR_WIDTH, runtimes, width=BAR_WIDTH, label=cache_type, color=COLORS[i])

        plt.xlabel("Filters")
        plt.ylabel("Average Runtime (in seconds)")
        plt.title("Average Cache Performance Across Filters")
        plt.xticks(FILTER_POSITIONS + (len(CACHES) - 1) * BAR_WIDTH / 2, FILTERS_.values(), rotation=45, ha="right")
        plt.legend(CACHES.keys())
        plt.savefig("plots/cache_across_filts.png")
        plt.show()
    

    logging.getLogger().setLevel(logging.INFO) 


def one_cache_all_filters(graph=False, log=True):
    if not log:
        logging.getLogger().setLevel(logging.CRITICAL) 

    logging.info("\n\n\n\nALL FILTERS IN A ROW FOR EACH CACHE TYPE: \n\n")

    data = []
    for cache_type, cache in CACHES.items():
        total_time = 0
        for filt in FILTERS_:
            start_time = time.perf_counter()
            catalog.load_table('iceberg_tutorial_db.nyc_taxi_iceberg').scan(row_filter=filt).to_arrow(cache)
            end_time = time.perf_counter()

            total_time += end_time - start_time
        data.append(total_time)
        logging.info(f"{cache_type}: {(total_time):.6f} seconds, {cache.get_cache_byte_size()} bytes")
        
        cache.empty()
    logging.info("\n-----------------------------------------------------------\n")

    if graph:
        plt.bar(CACHES.keys(), data, color=COLORS[1])
        plt.xlabel("Cache Type")
        plt.ylabel("Runtime (in seconds)")
        plt.title("Cache Performance With Different Filters Applied Sequentially")
        plt.savefig("plots/cache_with_sequential_filts.png")
        plt.show()

    logging.getLogger().setLevel(logging.INFO) 


def one_cache_one_limit_repeat_10(graph=False, log=True):
    if not log:
        logging.getLogger().setLevel(logging.CRITICAL)

    logging.info("\n\n\n\nDIFFERENT LIMITS FOR CACHES: \n\n")

    all_data = {}
    for cache_type, cache in CACHES.items():
        all_data[cache_type] = []
        for limit in LIMITS_:
            total_time = 0
            for _ in range(10):
                start_time = time.perf_counter()
                catalog.load_table('iceberg_tutorial_db.nyc_taxi_iceberg').scan(limit=limit).to_arrow(cache)
                end_time = time.perf_counter()

                total_time += end_time - start_time
            all_data[cache_type].append(total_time/10)
            logging.info(f"{cache_type} (limit={limit}): {(total_time/10):.6f} seconds, {cache.get_cache_byte_size()} bytes")
            
            cache.empty()
        logging.info("\n-----------------------------------------------------------\n")


    if graph:
        for i, (cache_type, runtimes) in enumerate(all_data.items()):
            plt.bar(LIMITS_POSITIONS + i * BAR_WIDTH, runtimes, width=BAR_WIDTH, label=cache_type, color=COLORS[i])

        plt.xlabel("Limits")
        plt.ylabel("Average Runtime (in seconds)")
        plt.title("Average Cache Performance Across Limits")
        plt.xticks(LIMITS_POSITIONS + (len(CACHES) - 1) * BAR_WIDTH / 2, LIMITS_.values(), rotation=45, ha="right")
        plt.legend(CACHES.keys())
        plt.savefig("plots/cache_across_limits.png")
        plt.show()

    logging.getLogger().setLevel(logging.INFO) 

# def one_cache_multiple_catalogs(graph=False, log=True):
#     if not log:
#         logging.getLogger().setLevel(logging.CRITICAL) 

#     all_data = {}
#     logging.info("\n\n\n\nDIFFERENT CATALOGS: \n\n")

#     if graph:
#         for i, (cache_type, runtimes) in enumerate(all_data.items()):
#             plt.bar(FILTER_POSITIONS + i * BAR_WIDTH, runtimes, width=BAR_WIDTH, label=cache_type, color=COLORS[i])

#         plt.xlabel("Caches Different Filters")
#         plt.ylabel("Runtime (in seconds)")
#         plt.title("Cache Performance Across Filters")
#         plt.xticks(np.array, CACHES.keys()) 
#         plt.legend()
#         plt.savefig("plots/cache_across_filts.png")
#         plt.show()

#     logging.getLogger().setLevel(logging.INFO) 

