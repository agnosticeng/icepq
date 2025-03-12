create temporary table iceberg_table_merge_candidates as (
    with 
        '{{.S3_ACCESS_KEY_ID}}' as s3_access_key_id,
        '{{.S3_SECRET_ACCESS_KEY}}' as s3_secret_access_key,
        '{{.TABLE_LOCATION}}' as table_location,
        '{{.MAX_MERGE_INPUT_FILES | default(50) }}' as max_merge_input_files,
        '{{.MAX_MERGER_OUTPUT_SIZE | default(10737418240) }}' as max_merge_output_size, -- default=10GB

        q0 as (
            select 
                _file as file,
                any(_size) as size,
                any(_time) as time
            from iceberg(table_location, s3_access_key_id, s3_secret_access_key)
            where _size < max_merge_output_size
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
            where total_size <= max_merge_output_size
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
