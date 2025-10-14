import math
import time
import boto3
import os
import uuid

# Parameters

database_name = 'tsp_vehicle_countly_c1_pre'
table_name = 't_countly_events'

# CSV 文件
csv_s3_bucket = 'emr-bjs-9471'
csv_s3_object_prefix = 'data/databases/ods_govern/pupumall_dc_sandbox_use_2'

rows_count = 20
rows_per_batch = 10

partitions_count = 3

run_id = str(int(time.time()*1000))

row_global_id = 1

def log(message):
    print(message + "\n")

def generate_data(batch_id, batches_count, rows_this_batch):
    log(f"---------- Running generate_data({batch_id}/{batches_count}/{rows_this_batch})... ----------")

    data = {}

    row_batch_id = 1

    global row_global_id

    while row_batch_id <= rows_this_batch:
        item_postfix = "-" + run_id + "-" + str(batch_id) + "-" + str(row_batch_id) + "-" + str(row_global_id)
        partition_postfix = "-" + str(row_global_id % partitions_count)

        bucket = "bucket" + item_postfix
        key = "key" + item_postfix
        size = "size" + item_postfix
        last_modified_date = "last_modified_date" + item_postfix
        storage_class = "storage_class" + item_postfix
        intelligent_tiering_access_tier = "intelligent_tiering_access_tier" + item_postfix

        dt = "dt" + partition_postfix

        item = f"{bucket},{key},{size},{last_modified_date},{storage_class},{intelligent_tiering_access_tier}\n"

        data[dt] = data.get(dt, '') + item

        row_batch_id = row_batch_id + 1
        row_global_id = row_global_id + 1

    log(f"---------- Running generate_data({batch_id}/{batches_count}/{rows_this_batch})...Done. ----------")

    return data

def write_s3(batch_id, batches_count, s3, data):

    rows_this_batch = len(data)

    log(f"---------- Running write_s3({batch_id}/{batches_count}/{rows_this_batch})... ----------")

    csv_path_local = "csv.tmp"

    for key, value in data.items():
        
        # 写 csv 文件
        with open(csv_path_local, 'a', encoding='utf-8') as file:
            file.writelines(value)
            file.flush()
        
        # 上传 csv 文件到 S3
        csv_s3_object_key = f"{csv_s3_object_prefix}/dt={key}/{run_id}_{uuid.uuid4()}.csv"
        s3.upload_file(csv_path_local, csv_s3_bucket, csv_s3_object_key)

        # 删除本地 csv 文件
        os.remove(csv_path_local)

    log(f"---------- Running write_s3({batch_id}/{batches_count}/{rows_this_batch})...Done. ----------")

def batch_write_s3(batch_id, batches_count, rows_this_batch, s3):
    log(f"========== Running batch_write_s3({batch_id}/{batches_count}/{rows_this_batch})... ==========")

    # 生成数据
    data = generate_data(batch_id, batches_count, rows_this_batch)

    # 写 S3
    write_s3(batch_id, batches_count, s3, data)

    log(f"========== Running batch_write_s3({batch_id}/{batches_count}/{rows_this_batch})...Done. ==========")

def main():

    # s3 client.
    
    s3 = boto3.client('s3')

    # Batches
    
    batches_count = math.ceil(rows_count/rows_per_batch)
    rows_the_last_batch = rows_count % rows_per_batch

    batch_id = 1

    # 达到 rows_per_batch 的完整批次
    while batch_id < batches_count:
        batch_write_s3(batch_id, batches_count, rows_per_batch, s3)
        batch_id = batch_id + 1
    
    # 最后一个批次，可能够 rows_per_batch，可能不够 rows_per_batch
    if rows_the_last_batch==0:
        batch_write_s3(batch_id, batches_count, rows_per_batch, s3)
    else:
        batch_write_s3(batch_id, batches_count, rows_the_last_batch, s3)

if __name__ == "__main__":
    main()
