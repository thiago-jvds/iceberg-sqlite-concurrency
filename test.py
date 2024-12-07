from pyiceberg.catalog import load_catalog
import boto3
import pandas as pd
import io
import pyarrow.parquet as pq
import time

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

file_path = catalog.load_table(
    'iceberg_tutorial_db.nyc_taxi_iceberg').scan(FILTER_).plan_files()[0].file.file_path

print(
    'file_path:', file_path
)

# exclude 's3://<bucket_name>/' out of the path
file_key = file_path[len('s3://') + len(bucket_name) + 1:]

# get the file
response = s3.get_object(Bucket=bucket_name, Key=file_key)

# Read the file content
file_content = io.BytesIO(response['Body'].read())

start_time = time.perf_counter()
catalog.load_table(
    'iceberg_tutorial_db.nyc_taxi_iceberg').scan(FILTER_).to_arrow()
end_time = time.perf_counter()
print(f"normal iceberg retrieval took {end_time - start_time:.6f} seconds")

# Measure time for function_2
start_time = time.perf_counter()
pq.read_table(file_content)
end_time = time.perf_counter()
print(f"cached file retrieval took {end_time - start_time:.6f} seconds")
