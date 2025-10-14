import math
import time

import boto3

# Parameters

database_name = 'tsp_vehicle_countly_c1_pre'
table_name = 't_countly_events'

output_location = 's3://athena-bjs-9471/output/'

rows_count = 10000
rows_per_batch = 1000

partitions_count = 10

run_id = str(int(time.time()*1000))

row_global_id = 1

def log(message):
    print(message + "\n")

def generate_data(batch_id, batches_count, rows_this_batch):
    log(f"---------- Running generate_data({batch_id}/{batches_count}/{rows_this_batch})... ----------")

    data = f"insert into {database_name}.{table_name} values"

    row_batch_id = 1

    global row_global_id

    while row_batch_id <= rows_this_batch:
        item_postfix = "-" + run_id + "-" + str(batch_id) + "-" + str(row_batch_id) + "-" + str(row_global_id)
        partition_postfix = "-" + str(row_global_id % partitions_count)

        appid = "appid" + item_postfix
        eventkey = "eventkey" + item_postfix
        collectionname = "collectionname" + item_postfix
        countlyuserid = "countlyuserid" + item_postfix
        deviceid = "deviceid" + item_postfix
        lastsessionid = "lastsessionid" + item_postfix

        appname = "appname" + partition_postfix
        p_date = "p_date" + partition_postfix

        item = f"('{appid}','{eventkey}','{collectionname}','{countlyuserid}','{deviceid}','{lastsessionid}','{appname}','{p_date}')"

        data = f"{data}{item},"

        row_batch_id = row_batch_id + 1
        row_global_id = row_global_id + 1
    
    # 去除最后一个逗号
    data = data[:-1]

    log(f"---------- Running generate_data({batch_id}/{batches_count}/{rows_this_batch})...Done. ----------")

    return data

def write_athena(batch_id, batches_count, rows_this_batch, athena, data):

    log(f"---------- Running write_athena({batch_id}/{batches_count}/{rows_this_batch})... ----------")

    response = athena.start_query_execution(
        QueryString=data,
        WorkGroup='primary',
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )

    log(f"response: {response}")

    log(f"---------- Running write_athena({batch_id}/{batches_count}/{rows_this_batch})...Done. ----------")

def batch_write_athena(batch_id, batches_count, rows_this_batch, athena):
    log(f"========== Running batch_write_athena({batch_id}/{batches_count}/{rows_this_batch})... ==========")

    # 生成数据
    data = generate_data(batch_id, batches_count, rows_this_batch)

    # 写 Athena
    write_athena(batch_id, batches_count, rows_this_batch, athena, data)

    log(f"========== Running batch_write_athena({batch_id}/{batches_count}/{rows_this_batch})...Done. ==========")

def main():

    # Athena client.
    
    athena = boto3.client('athena')

    # Batches
    
    batches_count = math.ceil(rows_count/rows_per_batch)
    rows_the_last_batch = rows_count % rows_per_batch

    batch_id = 1

    # 达到 rows_per_batch 的完整批次
    while batch_id < batches_count:
        batch_write_athena(batch_id, batches_count, rows_per_batch, athena)
        batch_id = batch_id + 1
    
    # 最后一个批次，可能够 rows_per_batch，可能不够 rows_per_batch
    if rows_the_last_batch==0:
        batch_write_athena(batch_id, batches_count, rows_per_batch, athena)
    else:
        batch_write_athena(batch_id, batches_count, rows_the_last_batch, athena)

if __name__ == "__main__":
    main()
