insert into table function s3('http://localhost:9001/test01/table01/data/{_partition_id}.parquet', 'minio', 'minio123')
partition by file
select
    rowNumberInAllBlocks() % 10 as file,
    *
from generateRandom('
    date Date,
    name String,
    value Float64,
    values Array(UInt64),
    metadata Map(String, String)
')
limit 100000
settings s3_create_new_file_on_insert=true