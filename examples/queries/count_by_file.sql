SELECT
    count(*),
    groupUniqArray(_file)
FROM iceberg('http://localhost:9001/test01/table_test_01', 'minio', 'minio123')