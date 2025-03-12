create temporary table iceberg_merge_candidates as (
    with 
        'minio' as access_key_id,
        'minio123' as secret_access_key,
        'http://localhost:9001/test01/table_test_01' as location,
        2 as max_merge_input_files,
        (400 * 1024 * 1024) as max_target_size,

        q0 as (
            select 
                _file as file,
                any(_size) as size,
                any(_time) as time
            from iceberg(location, access_key_id, secret_access_key)
            where _size < max_target_size
            group by file
            order by time desc
        ),

        q1 as (
            select
                arraySum(x -> coalesce(x.size, 0), input_files) as total_size,
                groupArray(
                    tuple(file as file, size as size, time as time)
                ) over (order by time asc rows between current row and unbounded following) as input_files
            from q0
        ),

        q2 as (
            select 
                *
            from q1
            where total_size <= max_target_size
            and length(input_files) > 1
        ),

        q3 as (
            select
                max(total_size) as total_size,
                argMax(q2.input_files, q2.total_size) as input_files
            from q2
            group by q2.input_files[-1].file
        ),

        q4 as (
            select
                total_size,
                generateUUIDv7() || '.parquet' as file,
                input_files,
                '{' || arrayStringConcat(arrayMap(x -> x.file, input_files), ',') || '}' as input_files_glob
            from q3
        )

    select * from q4
)

settings 
    enable_named_columns_in_function_tuple=1,
    max_threads=1

-------------------------------------------------------------------------------



-- select DISTINCT _file from file('folder/*.parquet', 'One')

-- WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;




with
    'minio' as access_key_id,
    'minio123' as secret_access_key,
    'http://localhost:9001/test01/table_test_01' as location,
    (select * from iceberg_merge_candidates limit 1 offset 0) as candidate,
    (location || '/data/' || candidate.file) as output_url,
    (location || '/data' || candidate.input_files_glob) as input_url,
    input as (select * from s3(input_url, access_key_id, secret_access_key))

insert into function s3(materialize(output_url), access_key_id, secret_access_key) 
select * from input



with 
    (
        select input_files_glob from iceberg_merge_candidates limit 1 offset 0
    ) as q0,

    q1 as (
        select * from s3(
            'http://localhost:9001/test01/table_test_01' || '/data/' || q0,
            'minio',
            'minio123'
        )
    )

insert into function s3(
    'http://localhost:9001/test01/table_test_01/data/coco.parquet',
    'minio',
    'minio123'
)
select * from q1






create view iceberg_buf_view_0 as (
    with 
        (select input_files_glob from iceberg_merge_candidates limit 1 offset 0) as pat

    select * from s3(
        'http://localhost:9001/test01/table_test_01' || '/data/' || pat,
        'minio',
        'minio123'
    )
)