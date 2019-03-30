import requests
import json
import pymysql.cursors
import numpy as np
import pandas as pd

import datetime


def date_parser(x):
    return datetime.datetime.strptime(x, '%Y-%m-%d')


def float_parser(x):
    res = 0
    try:
        res = float(x)
    except:
        res = 0

    return res


def lambda_handler(event, context):
    # Connect to the database
    connection = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='',
                                 db='aws_lambda_bitmex',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "CREATE TABLE IF NOT EXISTS `downloaded`(`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `date` varchar(255) DEFAULT NULL, `open` double DEFAULT NULL, `high` double DEFAULT NULL, `low` double DEFAULT NULL, `close` double DEFAULT NULL, `volume` double DEFAULT NULL, `num_3` double DEFAULT NULL, `num_3i` double DEFAULT NULL, `num_6` double DEFAULT NULL, `num_6i` double DEFAULT NULL, `num_9` double DEFAULT NULL, `num_9i` double DEFAULT NULL, `num_100` double DEFAULT NULL, `num_100i` double DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
            cursor.execute(sql, None)

        connection.commit()
    except:
        # print(connection.cursor()._last_executed)
        return {
            'statusCode': 500
        }

    url = "https://www.bitmex.com/api/v1/trade/bucketed"

    # defining a params dict for the parameters to be sent to the API
    params = {
        'binSize': '5m',
        'partial': 'false',
        'symbol': 'XBTUSD',
        'count': 750,
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

    date_arr = []
    open_arr = []
    for r in range(0, r_cnt):
        date_arr.append(date_parser(rows[r]['timestamp'][:10]))
        open_arr.append(float_parser(rows[r]['open']))

    data_setJson = {
        'date': date_arr,
        'open': open_arr
    }

    dataset_ex_df = pd.DataFrame(data_setJson)
    data_FT = dataset_ex_df[['date', 'open']]
    close_fft = np.fft.fft(np.asarray(data_FT['open'].tolist()))
    fft_df = pd.DataFrame({'fft': close_fft})
    fft_df['absolute'] = fft_df['fft'].apply(lambda x: np.abs(x))
    fft_df['angle'] = fft_df['fft'].apply(lambda x: np.angle(x))

    fft_list = np.asarray(fft_df['fft'].tolist())

    idx = 0
    for num_ in [3, 6, 9, 100]:
        print('---------------------------------------------------------------------------')
        fft_list_m10 = np.copy(fft_list);
        fft_list_m10[num_:-num_] = 0
        if idx == 0:
            num_3 = np.fft.ifft(fft_list_m10)
        elif idx == 1:
            num_6 = np.fft.ifft(fft_list_m10)
        elif idx == 2:
            num_9 = np.fft.ifft(fft_list_m10)
        elif idx == 3:
            num_100 = np.fft.ifft(fft_list_m10)

        idx += 1

    try:
        with connection.cursor() as cursor:
            # Create a new record
            for r in range(0, r_cnt):
                sql = "INSERT INTO `downloaded`(`date`, `open`, `high`, `low`, `close`, `volume`, " + \
                      "`num_3`, `num_3i`, `num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`) " + \
                      "VALUES (%s, %s, %s, %s, %s, %s, " + \
                      "%s, %s, %s, %s, %s, %s, %s, %s);"
                cursor.execute(sql, (
                    rows[r]['timestamp'][:10], float_parser(rows[r]['open']), float_parser(rows[r]['high']),
                    float_parser(rows[r]['low']), float_parser(rows[r]['close']), float_parser(rows[r]['volume']),
                    # 0, 0, 0, 0, 0, 0, 0, 0))
                    float_parser(num_3[r].real), float_parser(num_3[r].imag), float_parser(num_6[r].real), float_parser(num_6[r].imag),
                    float_parser(num_9[r].real), float_parser(num_9[r].imag), float_parser(num_100[r].real), float_parser(num_100[r].imag)))

        connection.commit()
    except:
        print(connection.cursor()._last_executed)
        return {
            'statusCode': 500
        }
    finally:
        connection.close()

    return {
        'statusCode': 200,
        'body': 'ok',
    }
    # csv_cols = []
    # csv_arr = []
    # for r in range(0, r_cnt):
    #     row = rows[r]
    #     arr = []
    #     csv_cols = []
    #     for key, val in row.items():
    #         csv_cols.append(key)
    #         arr.append(val)
    #     csv_arr.append(arr)
    #
    # print(csv_cols)
    # print(csv_arr)
    # with open('./result.csv', 'w', newline='') as writeFile:
    #     writer = csv.writer(writeFile)
    #     writer.writerows(csv_arr)
    #
    # writeFile.close()


print(lambda_handler(None, None))
