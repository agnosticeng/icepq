import duckdb

if __name__ == '__main__':
    conn = duckdb.connect()
    conn.install_extension("iceberg")
    conn.sql("""
        create or replace secret secret (
            type s3, 
            url_style 'path', 
            endpoint 'minio:9000',
            key_id 'minio',
            secret 'minio123',
            use_ssl false
        )
    """).show()

    conn.sql("select count(*) from iceberg_scan('s3://test/test_01', version_name_format='%s')").show()