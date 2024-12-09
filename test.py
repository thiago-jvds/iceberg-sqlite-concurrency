from pyiceberg.catalog import load_catalog
from pyiceberg.cache import LRUCache

import boto3
import pandas as pd
import io
import pyarrow.parquet as pq
import pyarrow
import time

CACHE = LRUCache()

# Initialize S3 client
s3 = boto3.client('s3')

glue_database_name = 'iceberg_tutorial_db'
glue_catalog_uri = 's3://pyiceberg-proj-bucket/nyc-taxi-iceberg'
catalog = load_catalog("glue", **{"type": "glue",
                                  "s3.region": "us-east-1",
                                  })

bucket_name = 'pyiceberg-proj-bucket'
# reduce file plans to just one
FILTER_ = 'passenger_count == 7'


def query_s3(key):
    # get the file
    response = s3.get_object(Bucket=bucket_name, Key=key)

    # Read the file content
    return io.BytesIO(response['Body'].read())


total_time = 0
for i in range(10):
    start_time = time.perf_counter()
    file_path = catalog.load_table(
        'iceberg_tutorial_db.nyc_taxi_iceberg').scan().plan_files()

    collected_files_content = []
    for data_file in file_path:
        path_ = data_file.file.file_path[len('s3://') + len(bucket_name) + 1:]

        if CACHE.get(path_) != -1:
            collected_files_content.append(CACHE.get(path_))
        else:
            file_content = query_s3(path_)
            CACHE.put(path_, file_content)
            collected_files_content.append(CACHE.get(path_))

    tables = []
    for file_content in collected_files_content:
        arrow_table = pq.read_table(file_content)
        tables.append(arrow_table)

    final_table = pyarrow.concat_tables(tables)
    end_time = time.perf_counter()
    total_time += end_time - start_time
print(f"cached retrieval took {(total_time/10):.6f} seconds")
print(f"cache size in bytes: {CACHE.get_cache_byte_size()} bytes")

for i in range(10):
    start_time = time.perf_counter()
    catalog.load_table(
        'iceberg_tutorial_db.nyc_taxi_iceberg').scan().to_arrow()
    end_time = time.perf_counter()
    total_time += end_time - start_time
print(f"normal retrieval took {(total_time/10):.6f} seconds")


# # exclude 's3://<bucket_name>/' out of the path
# file_key = file_path[len('s3://') + len(bucket_name) + 1:]
# print('file size:', file_content.getbuffer().nbytes)

# start_time = time.perf_counter()
# catalog.load_table(
#     'iceberg_tutorial_db.nyc_taxi_iceberg').scan(FILTER_).to_arrow()
# end_time = time.perf_counter()
# print(f"normal iceberg retrieval took {end_time - start_time:.6f} seconds")

# # Measure time for function_2
# start_time = time.perf_counter()
# pq.read_table(file_content)
# end_time = time.perf_counter()
# print(f"cached file retrieval took {end_time - start_time:.6f} seconds")
