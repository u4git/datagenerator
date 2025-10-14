import math
import time
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
import os
import boto3
from datetime import datetime

num_rows = 4000 * 10000
num_files = 4

s3_bucket_name = "glue-bjs-9471"
s3_object_prefix = "databases/db4data/table4parquet"

filename_prefix = f"data_{int(time.time()*1000)}"
filename_postfix = ".parquet"

num_rows_per_file = math.ceil(num_rows/num_files)
num_rows_per_batch = min(num_rows_per_file, 10000)

num_batches_per_file = math.floor(num_rows_per_file/num_rows_per_batch)

num_rows_last_per_file = num_rows_per_file - num_rows_per_batch * num_batches_per_file

columns = ["id", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"]

s3 = boto3.client('s3')

def log(message):
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + " " + str(message) + "\n")

def generate_values(batch_id, num_rows_this_batch, col_name):
    return [f"{col_name}_{batch_id}_{i}_{time.time()}" for i in range(num_rows_this_batch)]

def generate_data(batch_id, num_rows_this_batch):
    data = {}

    for col_name in columns:
        data[col_name] = generate_values(batch_id, num_rows_this_batch, col_name)
    
    return data

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

    # Full batches.
    for i in range(num_batches_per_file):
        data = generate_data(f"{file_id}_{i}", num_rows_per_batch)
        write_data(file_path, data)
    
    # Part batch.
    if num_rows_last_per_file > 0:
        data = generate_data(f"{file_id}_{num_batches_per_file}", num_rows_last_per_file)
        write_data(file_path, data)

    # Upload to S3.
    s3.upload_file(file_path, s3_bucket_name, f"{s3_object_prefix}/{file_path}")

    # Delete local file.
    os.remove(file_path)
    
    log(f"generate_file() file_path: {file_path}...done")

    return f"The file {file_path} has been done!"

def generate_files():
    log("generate_files()...")

    with ThreadPoolExecutor(max_workers=num_files) as executor:
        futures = [executor.submit(generate_file, file_id) for file_id in range(num_files)]
        
        for future in futures:
            log(future.result())
    
    log("generate_files()...done.")

def main():
    generate_files()

if __name__ == "__main__":
    main()