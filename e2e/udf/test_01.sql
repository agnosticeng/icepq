-- reload_udfs

system reload functions

;;

-- create_parquet_files

insert into table function s3('http://minio:9000/test/test_01/data/{_partition_id}.parquet', 'minio', 'minio123')
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

;;

-- append_parquet_files_to_iceberg_table

select icepq_append('s3://test/test_01', [
    '0.parquet',
    '1.parquet',
    '2.parquet',
    '3.parquet',
    '4.parquet',
    '5.parquet',
    '6.parquet',
    '7.parquet',
    '8.parquet',
    '9.parquet'
])

;;

-- check_table_count_and_files

select
    throwIf(count(*) != 100000),
    throwIf(arraySort(groupUniqArray(_file)) != [
    '0.parquet',
    '1.parquet',
    '2.parquet',
    '3.parquet',
    '4.parquet',
    '5.parquet',
    '6.parquet',
    '7.parquet',
    '8.parquet',
    '9.parquet'
])
from iceberg('http://minio:9000/test/test_01', 'minio', 'minio123')

;;

-- merge_parquet_files

insert into function s3('http://minio:9000/test/test_01/data/10.parquet', 'minio', 'minio123')
select
    *
from s3('http://minio:9000/test/test_01/data/{1..2}.parquet', 'minio', 'minio123')
settings 
    s3_create_new_file_on_insert=true,
    schema_inference_make_columns_nullable='auto'

;;

-- replace_parquet_files_in_iceberg_table

select icepq_replace(
    's3://test/test_01', 
    ['1.parquet', '2.parquet'],
    ['10.parquet']
)

;;

-- check_table_count_and_files

select
    throwIf(count(*) != 100000),
    throwIf(arraySort(groupUniqArray(_file)) != [
    '0.parquet',
    '10.parquet',
    '3.parquet',
    '4.parquet',
    '5.parquet',
    '6.parquet',
    '7.parquet',
    '8.parquet',
    '9.parquet'
])
from iceberg('http://minio:9000/test/test_01', 'minio', 'minio123')