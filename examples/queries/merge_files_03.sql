insert into function s3('http://localhost:9001/test01/table01/data/12.parquet', 'minio', 'minio123')
select
    *
from s3('http://localhost:9001/test01/table01/data/{6..7}.parquet', 'minio', 'minio123')
settings 
    s3_create_new_file_on_insert=true,
    schema_inference_make_columns_nullable='auto'
