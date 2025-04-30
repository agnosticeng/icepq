import polars as pl

if __name__ == '__main__':
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

