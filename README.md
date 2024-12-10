# PyIceberg with Cache

Manage your tables with swiftness with the addition of Parquet file caching in PyIceberg.

## Description
Iceberg is an open table format for large-scale data lakes, that has become increasingly suitable for huge analytics tables (repo for [PyIceberg](https://github.com/apache/iceberg-python), its Python implementation). 
In this repository, we extend its functionality in Python to allow for data file caching. This provides considerable speed-up for users who do not rely on engines like Spark to read the tables but do use cloud services to store Iceberg files.

## Table of Contents
1. [Installation](#installation)
2. [Usage](#usage)
3. [Features](#features)
4. [Limitations](#limitations)

## Installation 

- Start a Python enviroment
  1. Install venv if you don't have it already: `pip install virtualenv`
  2. Create a new environment if one doesn't exist: `python -m venv env`
  3. Activate it: `source env/bin/activate`
- Download the requirements for the code: `pip install -r requirements.txt`
- Build the relevant Cython code required for PyIceberg using: `python build-module.py`
- Set up a S3, Athena and Glue combo (tutorial with NYC taxi dataset we used: https://github.com/johnny-chivers/iceberg-athena-aws/blob/main/README.md) or other support storage and management systems.
- Create credentials to allow access to AWS server. You're looking for an `access_key` and a `secret_key`.
- Manually create credential file for AWS:
    1. `mkdir ~/.aws`
    2. ` cd ~/.aws`
    3. `vim credentials` to edit or create a file without any extensions called credentials. Add the following to this file:
    `
    [default]
    aws_access_key_id = <Your access key>
    aws_secret_access_key = <Your secret key>
    `
    4. Exit file by pressing the esc key and then typing `:wq` and hitting enter
    5. `vim config` to edit or create a file without any extensions called config. Add the following to this file:
    `
     [default]
    region = <Your AWS's nearest region>
    output = json
    `
    6. Exit the file using the same steps as described in above

## Usage
- You can re-generate the graphs used in the paper for the benchmarking section by running `python main.py` (it takes a few minutes to run. You can confirm it's running by looking at logfile.txt and ensuring that it's updating while the program is running.)
- You can add new files on the same level as main.py and import different PyIceberg functions and classes as normal (including our project's addition `from pyiceberg.cache import *`). You can then use these functions knowing that reads are going to be cached if a cache is initialized and passed to the relevant method (`.to_arrow(cache)`). The code in `test_caching_tables.py` can server as example code for you to build off of if needed.
- You can also test out the "next-step feature" caching files instead of tables by looking at and running `test_caching_files.py`. It uses a completely different set of libraries acting with PyIceberg and a general different pathway. However, it acts as a proof of concept. 

## New Features
Can now import and use LRU Cache and MRU Cache from `pyiceberg.cache`. You can also set the max number of items you'd like to be allowed into the cache. Pass the cache into the `.to_arrow` method to see the effects of the cache. If you don't it'll default to No Cache behavior. 

## Limitations
This project was developed to work with S3 cloud service and Glue catalog. Currently, other catalogs and cloud services are not tested and therefore, should be considered unstable.

## Considerations
This project was presented as the final project for MIT Fall 2024 6.5830 Database Management Systems class. 