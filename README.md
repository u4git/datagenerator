# Athena

## athena/csv_to_local_size.py

- 在本地生成 CSV 文件；
- 按指定的数据量大小生成；

## athena/csv_to_s3_rows_partitions.py

- 在 S3 上生成 CSV 文件；
- 按指定的行数生成；
- 自动在 S3 上生成分区目录；

## athena/csv_to_s3_rows_partitions_symlink.py

- 在 S3 上生成 CSV 文件；
- 按指定的行数生成；
- 自动在 S3 上生成分区目录；
- 自动生成 symlink 文件；

## athena/data_to_athena_rows_partitions.py

- 直接将数据插入到 Athena 中指定的表；
- 按指定的行数生成；
- 带分区信息；

# DynamoDB

## dynamodb/data_to_dynamodb_items.py

- 直接将数据插入到 DynamoDB 中指定的表；
- 按指定的行数生成；
