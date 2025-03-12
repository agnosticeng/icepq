import sys
from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq

if __name__ == '__main__':
    catalog = load_catalog('default')
    df = pq.read_table(sys.argv[1])

    catalog.create_namespace_if_not_exists("demo")

    table = catalog.create_table_if_not_exists(
        "demo.table01",
        schema=df.schema,
        location="s3://test01/table_test_02",
    )

    table.append(df)