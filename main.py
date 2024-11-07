from utils import setup_mock_tables, query_mock_table, cleanup_catalog


def main():
    try:
        catalog = setup_mock_tables()
        df = query_mock_table(catalog).to_pandas()
        print(df)
    finally:
        cleanup_catalog()


if __name__ == "__main__":
    main()
