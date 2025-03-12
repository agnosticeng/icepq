select
    _file,
    count(*)
from iceberg('http://localhost:9001/test01/table_test_01', 'minio', 'minio123')
group by _file
