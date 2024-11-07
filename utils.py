import os
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType
import pyarrow as pa
import pyarrow.parquet as pq
import shutil


def get_catalog(catalog_name="default", test_db_name="test.db"):
    """
    Creates and returns a SqlCatalog object for managing Iceberg tables.

    Args:
        catalog_name (str): The name of the catalog. Defaults to "default".
        test_db_name (str): The name of the SQLite database file. Defaults to "test.db".

    Returns:
        SqlCatalog: An instance of SqlCatalog configured with the specified catalog name and database.
    """
    warehouse_path = 'catalog'

    catalog = SqlCatalog(
        catalog_name,
        **{
            "uri": f"sqlite:///{warehouse_path}/{test_db_name}",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    
    if not catalog._namespace_exists("default"):
        catalog.create_namespace("default")
 
    return catalog

def setup_mock_tables():
    catalog = get_catalog()


    warehouse_path = "catalog"

    # if table doesn't exist already
    if tuple(["default", "test_table"]) not in catalog.list_tables("default"):
        schema = Schema(
            NestedField(1, 'name', StringType(), required=True),
            NestedField(2, 'age', IntegerType(), required=True),
            NestedField(3, 'year', IntegerType(), required=True)
        )
        table = catalog.create_table(
                identifier=f"default.test_table",
                schema=schema,
                location=f"file://{warehouse_path}/test_table"
            )

        arrow_schema = pa.schema([
            pa.field('name', pa.string(), nullable=False),
            pa.field('age', pa.int32(), nullable=False),
            pa.field('year', pa.int32(), nullable=False)
        ])

        data = pa.table({
            'name': ['john', 'marie'],
            'age': [25, 23],
            'year': [2014, 2017]
        }, schema=arrow_schema)

        data_dir = os.path.join(warehouse_path, 'test_table')
        os.makedirs(data_dir, exist_ok=True)

        data_file_path = os.path.join(data_dir, 'data.parquet')
        pq.write_table(data, data_file_path)

        df = pq.read_table(data_file_path)
        table.append(df)

    return catalog

def cleanup_catalog(catalog_name="default", test_db_name="test.db"):
    """
    Cleans up the Iceberg catalog by removing all tables, metadata, and namespaces, 
    and deletes the associated files.

    Args:
        catalog_name (str): The name of the catalog. Defaults to "default".
        test_db_name (str): The name of the SQLite database file. Defaults to "test.db".
    """
    # Initialize catalog
    warehouse_path = "catalog"
    catalog = SqlCatalog(
        catalog_name,
        **{
            "uri": f"sqlite:///{warehouse_path}/{test_db_name}",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    # List all tables in the default namespace and drop them
    if catalog._namespace_exists("default"):
        tables = catalog.list_tables("default")
        for table_name in tables:
            catalog.drop_table(table_name)
        catalog.drop_namespace("default")

    # Remove warehouse directory and SQLite database file
    if os.path.exists(warehouse_path):
        for item in os.listdir(warehouse_path):
            item_path = os.path.join(warehouse_path, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)  # Remove subdirectory
            else:
                os.remove(item_path)  # Remove file
    
def query_mock_table(catalog):
    """
    Queries data from the 'default.test_table' in the provided catalog.

    Args:
        catalog (SqlCatalog): The catalog instance returned from setup_mock_tables().

    Returns:
        pa.Table: A PyArrow Table containing the data from 'default.test_table'.
    """
    # Load the table
    table = catalog.load_table("default.test_table")

    # Perform a scan and fetch all data
    scan = table.scan()  # You can specify filters and columns here if needed
    arrow_table = scan.to_arrow()  # Convert the scan to a PyArrow Table

    return arrow_table