import math
import time
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
import os
import boto3
from datetime import datetime
import sys

bytes_files = 5000*1024*1024
num_files = 4

bytes_per_batch_default = 10*1024*1024

s3_bucket_name = "data-bjs-qa"
s3_object_prefix = "table4qa"

filename_prefix = f"data_{int(time.time()*1000)}"
filename_postfix = ".parquet"

bytes_per_file = math.ceil(bytes_files/num_files)
bytes_per_batch = min(bytes_per_file, bytes_per_batch_default)

num_batches_per_file = math.floor(bytes_per_file/bytes_per_batch)

bytes_last_per_file = bytes_per_file - bytes_per_batch * num_batches_per_file

columns = ["id", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"]

s3 = boto3.client('s3')

def log(message):
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + " " + str(message) + "\n")

def generate_data_attampt(batch_id, attampt_id, data):
    num_bytes = 0

    for col_name in columns:
        col_value = f"{col_name}_{batch_id}_{attampt_id}_{time.time()}"
        data[col_name].append(col_value)

        num_bytes += sys.getsizeof(col_value)
    
    return num_bytes

def generate_data_per_batch(batch_id, bytes_this_batch):
    data = {}

    for col_name in columns:
        data[col_name] = []
    
    num_bytes = 0
    num_rows = 0

    attampt_id = 0

    while num_bytes < bytes_this_batch:
        # 追加数据以满足当前批次大小
        num_bytes += generate_data_attampt(batch_id, attampt_id, data)
        # 一次 attampt 生成一行
        num_rows += 1

        attampt_id += 1
    
    log(f"data: {data}")

    return {"num_bytes":num_bytes, "num_rows":num_rows, "data":data}

def write_data(file_path, data):
    pa_table = pa.table(data)

    if os.path.exists(file_path):
        existing_table = pq.read_table(file_path)
        combined_table = pa.concat_tables([existing_table, pa_table])
        pq.write_table(combined_table, file_path)
    else:
        pq.write_table(pa_table, file_path)

def generate_file(file_id):
    file_path = f"{filename_prefix}_{file_id}{filename_postfix}"

    log(f"generate_file() file_path: {file_path} ...")
    
    num_bytes = 0
    num_rows = 0

    # Full batches.
    for i in range(num_batches_per_file):
        result = generate_data_per_batch(f"{file_id}_{i}", bytes_per_batch)
        data = result["data"]
        write_data(file_path, data)
        
        num_bytes += result["num_bytes"]
        num_rows += result["num_rows"]
    
    # Part batch.
    if bytes_last_per_file > 0:
        result = generate_data_per_batch(f"{file_id}_{num_batches_per_file}", bytes_last_per_file)
        data = result["data"]
        write_data(file_path, data)
        
        num_bytes += result["num_bytes"]
        num_rows += result["num_rows"]

    # Upload to S3.
    s3.upload_file(file_path, s3_bucket_name, f"{s3_object_prefix}/{file_path}")

    # Delete local file.
    os.remove(file_path)
    
    log(f"generate_file() file_path: {file_path}, num_bytes: {num_bytes}, num_rows: {num_rows}...done")

    return {"num_bytes":num_bytes, "num_rows":num_rows}

def generate_files():
    log("generate_files()...")

    log(f"s3_bucket_name: {s3_bucket_name}")
    log(f"s3_object_prefix: {s3_object_prefix}")
    log(f"bytes_files: {bytes_files}")
    log(f"num_files: {num_files}")
    log(f"bytes_per_file: {bytes_per_file}")
    log(f"bytes_per_batch: {bytes_per_batch}")
    log(f"num_batches_per_file: {num_batches_per_file}")
    log(f"bytes_last_per_file: {bytes_last_per_file}")
    
    num_bytes = 0
    num_rows = 0

    with ThreadPoolExecutor(max_workers=num_files) as executor:
        futures = [executor.submit(generate_file, file_id) for file_id in range(num_files)]
        
        for future in futures:
            result = future.result()
            num_bytes += result["num_bytes"]
            num_rows += result["num_rows"]
    
    log(f"generate_files() num_bytes: {num_bytes}, num_rows: {num_rows}...done.")

def main():
    generate_files()

if __name__ == "__main__":
    main()