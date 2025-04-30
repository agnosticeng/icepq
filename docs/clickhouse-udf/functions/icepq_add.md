### icepq_add

Add Parquet datafiles to an Iceberg table.

**Syntax**

```sql
icepq_add(table_location, files)
```

**Parameters**

- `table_location` - The root path of the Iceberg table. [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
- `files` - An array of Parquet files to add to the table. These must be filename, not path. The files must exists under the path `${table_location}/data/`.[Array(String)](https://clickhouse.com/docs/sql-reference/data-types/array)

**Returned value**

- Returns and emtpy string if the operation succeeded.

**Example**

Query:

```sql
select icepq_add('s3://mybucket/mytable', ['data1.parquet'])
```

Result:

| icepq_add('s3://mybucket/mytable', ['data1.parquet']) |
|-:|
||