insert into function s3('http://localhost:9001/test01/table_test_01/data/1.parquet', 'minio', 'minio123')
select
    *
from (
    select
        *
    from generateRandom('
        date Date,
        name String,
        value Float64
    ')
    limit 10000000
)