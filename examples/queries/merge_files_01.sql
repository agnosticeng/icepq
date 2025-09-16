insert into function s3('http://localhost:9001/test01/table01/data/10.parquet', 'minio', 'minio123')
select
    *
from s3('http://localhost:9001/test01/table01/data/{0..2}.parquet', 'minio', 'minio123')
settings 
    s3_create_new_file_on_insert=true,
    schema_inference_make_columns_nullable='auto'
