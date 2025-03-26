insert into function s3('http://localhost:9001/test01/table_test_01/data/data.parquet', 'minio', 'minio123')
select
    *
from (
    select
        *
    from generateRandom('
        date Date,
        name String,
        value Float64,
        values Array(UInt64),
        metadata Map(String, String)
    ')
    limit 1000000
)
settings s3_create_new_file_on_insert=true