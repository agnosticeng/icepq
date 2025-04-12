import polars as pl
import pyiceberg.io as io

if __name__ == '__main__':
    # conn = duckdb.connect()
    # conn.install_extension("iceberg")
    # conn.sql("""
    #     create or replace secret secret (
    #         type s3, 
    #         url_style 'path', 
    #         endpoint 'localhost:9001',
    #         key_id 'minio',
    #         secret 'minio123',
    #         use_ssl false
    #     )
    # """).show()
    
    # conn.sql("select * from iceberg_scan('s3://test01/table01', version_name_format='%s') limit 10").show()



    df = pl.scan_iceberg(
        "s3://test01/table01",
        storage_options={
            "s3.endpoint": "http://localhost:9001",
            "s3.region": "us-east-1",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123",
        }
    )

    print(df.collect())