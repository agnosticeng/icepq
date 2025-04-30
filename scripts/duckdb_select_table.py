import duckdb

if __name__ == '__main__':
    conn = duckdb.connect()
    conn.install_extension("iceberg")
    conn.sql("""
        create or replace secret secret (
            type s3, 
            url_style 'path', 
            endpoint 'localhost:9001',
            key_id 'minio',
            secret 'minio123',
            use_ssl false
        )
    """).show()
    
    conn.sql("select * from iceberg_scan('s3://test01/table01', version_name_format='%s%s') limit 10").show()