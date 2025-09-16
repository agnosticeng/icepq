#!/bin/bash

set -ex

clickhouse --queries-file ./examples/queries/generate_files.sql
for i in {0..9}; do ./bin/icepq table add s3://test01/table01 "$i.parquet"; done 
clickhouse --queries-file ./examples/queries/merge_files_01.sql
./bin/icepq table replace s3://test01/table01 0.parquet,1.parquet,2.parquet 10.parquet 
clickhouse --queries-file ./examples/queries/merge_files_02.sql
./bin/icepq table replace s3://test01/table01 4.parquet,5.parquet 11.parquet 
clickhouse --queries-file ./examples/queries/merge_files_03.sql
./bin/icepq table replace s3://test01/table01 6.parquet,7.parquet 12.parquet 
./bin/icepq table expire-snapshots --retain-last=1 --older-than=1ms s3://test01/table01