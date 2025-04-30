### icepq_add

Replace Parquet datafiles in an Iceberg table.

**Syntax**

```sql
icepq_replace(table_location, old_files, new_files)
```

**Parameters**

- `table_location` - The root path of the Iceberg table. [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
- `old_files` - An array of Parquet files to remove from the Iceberg table. These must be filename, not path. The files must exists under the path `${table_location}/data/`.[Array(String)](https://clickhouse.com/docs/sql-reference/data-types/array)
- `new_files` - An array of Parquet files to add to the Iceberg table. These must be filename, not path. The files must exists under the path `${table_location}/data/`.[Array(String)](https://clickhouse.com/docs/sql-reference/data-types/array)

**Returned value**

- Returns and emtpy string if the operation succeeded.

**Example**

Query:

```sql
select icepq_replace('s3://mybucket/mytable', ['data1.parquet', 'data2.parquet'], ['data3.parquet'])
```

Result:

| icepq_replace('s3://mybucket/mytable', ['data1.parquet', 'data2.parquet'], ['data3.parquet']) |
|-:|
||