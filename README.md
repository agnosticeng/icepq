# 🧊 icepq

**icepq** is a CLI tool and a set of ClickHouse **User-Defined Functions (UDFs)** to manage **Apache Iceberg** tables when working with **out-of-band Parquet files** — especially files produced by ClickHouse itself.

---

## ❄️ Why icepq?

Apache Iceberg is a powerful table format, but ClickHouse cannot write directly into Iceberg tables.  
**icepq** was created to bridge this gap and unlock new workflows:

- ✅ **Create Iceberg tables** without a catalog, directly on the filesystem.
- ✅ **Register Parquet files** produced by ClickHouse (or any system) into an Iceberg table.
- ✅ **Leverage SQL** to precisely control how Parquet files should be merged or compacted.
- ✅ **Update Iceberg metadata** from within ClickHouse using simple UDF calls.

With icepq, you can fully integrate Iceberg table maintenance into high-performance SQL pipelines without needing Spark, Flink, or external services.

---

## ✨ Features

- 📦 **Create** Iceberg tables without requiring an external catalog.
- ➕ **Add** new Parquet files to an existing Iceberg table.
- 🔄 **Replace** old Parquet files with new ones (e.g., after compaction).
- 🛠️ **UDF support**: manipulate Iceberg metadata directly from SQL queries.

---

## ClickHouse UDF functions

- [icepq_add](./docs/clickhouse-udf/functions/icepq_add.md)
- [icepq_replace](./docs/clickhouse-udf/functions/icepq_replace.md)

---

## 📦 Artifact: The Bundle

The output of the build process is distributed as a **compressed archive** called a **bundle**. This bundle includes everything needed to deploy and use the UDFs in ClickHouse.

### 📁 Bundle Contents

Each bundle contains:

- 🧩 **Standalone binary** implementing the native UDFs (compiled with ClickHouse compatibility)
- ⚙️ **ClickHouse configuration files** (`.xml`) to register each native UDF

### 📦 Bundle Usage

#### 🛠️ Build the Bundle

```sh
make bundle              # Build for native execution
GOOS=linux make bundle   # Cross-compile for use in Docker (Linux target)
```

This will:

- Generate the bundle directory at `tmp/bundle/`
- Create a compressed archive at `tmp/bundle.tar.gz`

The internal file structure of the bundle reflects the default layout of a basic ClickHouse installation.  
As a result, **decompressing the archive at the root of a ClickHouse server filesystem should "just work"** with no additional path configuration.

---

#### ▶️ Run with `clickhouse-local`

```sh
clickhouse local \
    --path tmp/clickhouse \
    -- \
    --user_scripts_path=./tmp/bundle/var/lib/clickhouse/user_scripts \
    --user_defined_executable_functions_config="./tmp/bundle/etc/clickhouse-server/*_function.*ml"
```

This runs ClickHouse in local mode using the provided config and a temporary storage path.

---

#### 🐳 Run in development mode with `clickhouse-server` in Docker

```sh
docker compose up -d
```

This launches a ClickHouse server inside a Docker container using the configuration and UDFs from the bundle.

---

## ⚠️ Limitations

- 📚 **No catalog support yet**:  
  icepq currently operates in a **catalog-less mode**, managing Iceberg metadata directly through filesystem operations.  
  It relies on the `version-hint.text` file to track table versions and does not integrate with external catalogs (e.g., Hive Metastore, AWS Glue, Nessie).

- 🧩 **No partitioning support**:  
  Tables managed with icepq must currently be **unpartitioned**.  
  Support for partitioned tables may be added in the future.

- 📁 **Strict file layout requirements**:  
  The file layout must follow the standard Iceberg conventions:
  - Data files must be stored under: `<table_location>/data/`
  - Metadata files must be stored under: `<table_location>/metadata/`

---

## Example workflow

Here we will use ClickHouse local with the newly created bundle.

```sh
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio123
export AWS_REGION=us-east-1
export AWS_S3_ENDPOINT=http://localhost:9001
export AWS_S3_FORCE_PATH_STLE=true

clickhouse local \
    --path tmp/clickhouse \
    -- \
    --user_scripts_path=./tmp/bundle/var/lib/clickhouse/user_scripts \
    --user_defined_executable_functions_config="./tmp/bundle/etc/clickhouse-server/*_function.*ml"
```

1. Create some Parquet files with ClickHouse

```sql
insert into table function s3('http://localhost:9001/test01/table01/data/{_partition_id}.parquet')
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
format Markdown
settings s3_create_new_file_on_insert=true
```

2. Create an Iceberg table from these files

```sql
select icepq_add('s3://test01/table01', [
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
]) format Markdown
```

3. Query your table 

```sql
select 
    count(*), 
    arraySort(groupUniqArray(_file))
from iceberg('http://localhost:9001/test01/table01') 
format Markdown
```

| count() | groupUniqArray(_file) |
|-:|:-|
| 200000 | ['0.parquet','8.parquet','6.parquet','4.parquet','2.parquet','3.parquet','5.parquet','9.parquet','7.parquet','1.parquet'] |

4. Merge some of the files into a bigger one

```sql
insert into function s3('http://localhost:9001/test01/table01/data/10.parquet')
select
    *
from s3('http://localhost:9001/test01/table01/data/{0..2}.parquet')
settings 
    s3_create_new_file_on_insert=true,
    schema_inference_make_columns_nullable='auto'
```

5. Replace merged file by the new big one in the Iceberg metadata

```sql
select icepq_replace(
    's3://test01/table01', 
    [
        '0.parquet', 
        '1.parquet', 
        '2.parquet'
    ],
    [
        '10.parquet'
    ]
) format Markdown
```

6. Query your table again

```sql
select 
    count(*), 
    arraySort(groupUniqArray(_file))
from iceberg('http://localhost:9001/test01/table01') 
format Markdown
```

| count() | arraySort(groupUniqArray(_file)) |
|-:|:-|
| 100000 | ['10.parquet','3.parquet','4.parquet','5.parquet','6.parquet','7.parquet','8.parquet','9.parquet'] |