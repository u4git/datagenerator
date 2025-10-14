import os
import sys
from datetime import datetime
import random

# 目标文件路径
file_path = "data0.csv"
# 目标文件大小
file_size_bytes = 1024 * 1024 * 100

# 分批落盘时，每一批数据的大小
batch_size_bytes = 1024 * 1024 * 10
# 为达到每批数据量大小，每次尝试生成的记录数量
batch_size_records = 500

def generate_array_string(prefix):
    return prefix + str(int(datetime.now().timestamp()*1000000)) + '-' + str(random.randint(1, 9))

def generate_data_batch_attempt(data):
    print('Executing generate_data_batch_attempt()...')

    index = 0
    while index < batch_size_records:
        data = data + generate_array_string('id-') + ',' + generate_array_string('name-') + ',' + '2025-05-01\n'
        index = index + 1
    
    print('Executing generate_data_batch_attempt()...Done.')

    return data

def generate_data_batch():
    print('Executing generate_data_batch()...')

    data = ''

    # 生成一批数据
    data = generate_data_batch_attempt(data)

    # 计算数据大小
    data_size = sys.getsizeof(data)

    while data_size < batch_size_bytes:
        # 追加一批数据，达到每批次的大小
        data = generate_data_batch_attempt(data)

        # 计算数据大小
        data_size = sys.getsizeof(data)
    
    print('Executing generate_data_batch()...Done.')

    return data

def generate_file():

    # 创建文件（包含列名）
    f = open(file_path, 'w')
    f.write("id,name,p_date\n")
    f.close()

    # 计算文件大小
    current_file_size_bytes = os.path.getsize(file_path)
    print(f'current_file_size_bytes: {current_file_size_bytes}')

    with open(file_path, 'a', encoding='utf-8') as file:
        
        while current_file_size_bytes < file_size_bytes:
            
            # 生成一批数据
            data_batch = generate_data_batch()

            # 写文件
            file.writelines(data_batch)
            file.flush()
            
            # 计算文件大小
            current_file_size_bytes = os.path.getsize(file_path)
            print(f'current_file_size_bytes: {current_file_size_bytes}')


if __name__ == "__main__":
    print("Generating data...")
    generate_file()
    print("Generating data...Done.")