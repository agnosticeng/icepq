select
    * 
from iceberg('http://localhost:9001/test01/table_test_01', 'minio', 'minio123')
limit 100