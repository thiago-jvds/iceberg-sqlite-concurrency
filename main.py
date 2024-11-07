import os
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow.compute as pc
import pyarrow.parquet as pq


def get_catalog(catalog_name="default", test_db_name="test.db"):
    """
    Creates and returns a SqlCatalog object for managing Iceberg tables.

    Args:
        catalog_name (str): The name of the catalog. Defaults to "default".
        test_db_name (str): The name of the SQLite database file. Defaults to "test.db".

    Returns:
        SqlCatalog: An instance of SqlCatalog configured with the specified catalog name and database.
    """
    warehouse_path = "catalog"
    catalog = SqlCatalog(
        catalog_name,
        **{
            "uri": f"sqlite:///{warehouse_path}/{test_db_name}",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    return catalog

def create_table(catalog, schema, table_name, namespace):
    table = catalog.create_table(
            f"{namespace}.{table_name}",
            schema=schema,
        )
    return table

def setup_mock_tables():
    pass

def main():
    catalog = get_catalog()
    catalog.create_namespace("default")
    



if __name__ == "__main__":
    main()
