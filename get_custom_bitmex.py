# import requests
import json
import csv


def lambda_handler(event, context):
    csvfile = open('test.csv', 'r')

    reader = csv.DictReader(csvfile)
    rows = []
    for row in reader:
        rows.append(json.dumps(row))

    # print(json.dumps(rows))
    return {
        'statusCode': 200,
        'body': json.dumps(rows)
    }


print(lambda_handler(None, None))
#
# a = [
#      ]
