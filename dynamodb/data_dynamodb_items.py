import boto3
from botocore.config import Config

import time

def main():

    my_config = Config(
        region_name = 'cn-north-1',
        signature_version = 'v4',
        retries = {
            'max_attempts': 10,
            'mode': 'standard'
        }
    )

    client = boto3.client('dynamodb', config=my_config)

    tableName = "table4copy"

    itemCountTotal = 200000
    itemCountPerBatch = 25

    batchCount = itemCountTotal / itemCountPerBatch

    batchNum = 1

    print("tableName: " + str(tableName))
    print("itemCountTotal: " + str(itemCountTotal))
    print("itemCountPerBatch: " + str(itemCountPerBatch))
    print("batchCount: " + str(batchCount))

    while batchNum <= batchCount:

        batch_write_item(batchNum, itemCountPerBatch, batchCount, client, tableName)

        batchNum = batchNum + 1

        time.sleep(1)


def batch_write_item(batchNum, itemCountPerBatch, batchCount, client, tableName):

    print("========== batch: " + str(batchNum) + "/" + str(batchCount) + " start ==========")

    batchRequest = {
        tableName: []
    }

    itemNum = 1

    while itemNum <= itemCountPerBatch:

        postFix = '-' + str(batchNum) + '-' + str(itemNum)

        putRequest = {
            'PutRequest': {
                'Item': {
                    'pk': {
                        'S': 'pk' + postFix
                    },
                    'sk': {
                        'S': 'sk' + postFix
                    },
                    'c0': {
                        'N': '100'
                    }
                }
            }
        }

        batchRequest[tableName].append(putRequest)

        itemNum = itemNum + 1

    response = client.batch_write_item(RequestItems=batchRequest)

    # print(response)

    print("========== batch: " + str(batchNum) + "/" + str(batchCount) + " end ==========")

if __name__ == '__main__':
    main()
