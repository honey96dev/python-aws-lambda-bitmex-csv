# importing the requests library
import requests
import json
import csv


def get_data():
    url = "https://www.bitmex.com/api/v1/trade/bucketed"

    # defining a params dict for the parameters to be sent to the API
    params = {
        'binSize': '5m',
        'partial': 'false',
        'symbol': 'XBTUSD',
        'count': 500,
        'reverse': 'false',
        'startTime': '2014-08-25T00:00:00.000Z',
    }

    # sending get request and saving the response as response object
    r = requests.get(url=url, params=params)

    formatted_string = r.text.replace("'", '"')
    rows = json.loads(formatted_string)
    r_cnt = len(rows)
    if r_cnt == 0:
        return

    csv_arr = []
    for r in range(0, r_cnt):
        row = rows[r]
        arr = []
        for key, val in row.items():
            arr.append(val)
        csv_arr.append(arr)

    print(csv_arr)
    with open('./result.csv', 'w', newline='') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerows(csv_arr)

    writeFile.close()


get_data()
#
# a = [
#      ]
