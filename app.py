import datetime
import json
import sched
import time
from datetime import timedelta
from threading import Thread

import numpy as np
import pandas as pd
import pymysql.cursors
import pymysql.cursors
import requests
from flask import Flask, abort

app = Flask(__name__)


def date_parser(x):
    return datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.000Z')


@app.route('/get_custom_bitmex/<interval>')
def get_custom_bitmex(interval):
    # connection = pymysql.connect(host='108.61.186.24',
    #                              user='bitmex3536',
    #                              #  password='',
    #                              password='BitMex*95645636',
    #                              db='aws_lambda_bitmex',
    #                              charset='utf8',
    #                              cursorclass=pymysql.cursors.DictCursor)
    connection = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='',
                                 db='aws_lambda_bitmex',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:

            sql = "SELECT `id`, `timestamp`, `open`, `high`, `low`, `close`, `volume`, `num_3`, `num_3i`, " + \
                  "`num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`, `date` FROM (SELECT D.* FROM `downloaded_" + interval + "` D ORDER BY D.timestamp DESC LIMIT 0, 2000) `sub` ORDER BY `timestamp` ASC;"
            cursor.execute(sql)
            rows1 = cursor.fetchall()
    except:
        # print(connection.cursor()._last_executed)
        connection.close()
        abort(401)

    date_arr = []
    open_arr = []

    result_cnt = len(rows1) - 1
    for r in rows1:
        date_arr.append(r['date'])
        open_arr.append(r['open'])

    for r in range(0, 60):
        date_arr.append(rows1[result_cnt]['date'])
        open_arr.append(rows1[result_cnt]['open'])

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

    rows = []
    cnt = len(rows1) - 1
    idx = 0
    for row in rows1:
        rows.append(row)
        rows[idx]['id'] = cnt - idx
        rows[idx]['num_3'] = num_3[idx].real
        rows[idx]['num_3i'] = num_3[idx].imag
        rows[idx]['num_6'] = num_6[idx].real
        rows[idx]['num_6i'] = num_6[idx].imag
        rows[idx]['num_9'] = num_9[idx].real
        rows[idx]['num_9i'] = num_9[idx].imag
        rows[idx]['num_100'] = num_100[idx].real
        rows[idx]['num_100i'] = num_100[idx].imag
        idx = idx + 1

    last_timestamp = date_parser(rows[cnt]['timestamp'])
    # print(last_timestamp)
    for i in range(0, 60):
        if interval == "1m":
            last_timestamp = last_timestamp + timedelta(minutes=1)
        elif interval == "5m":
            last_timestamp = last_timestamp + timedelta(minutes=5)
        elif interval == "1h":
            last_timestamp = last_timestamp + timedelta(hours=1)

        row1 = {
            'id': cnt - idx,
            'timestamp': last_timestamp.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'open': rows[cnt]['open'],
            'high': rows[cnt]['high'],
            'low': rows[cnt]['low'],
            'close': rows[cnt]['close'],
            'volume': rows[cnt]['volume'],
            'num_3': num_3[idx].real,
            'num_3i': num_3[idx].imag,
            'num_6': num_6[idx].real,
            'num_6i': num_6[idx].imag,
            'num_9': num_9[idx].real,
            'num_9i': num_9[idx].imag,
            'num_100': num_100[idx].real,
            'num_100i': num_100[idx].imag
        }
        rows.append(row1)
        idx = idx + 1

    result = {
        "result": rows
    }
    # result["result"] = rows
    # for row in rows:
    #     result.append(json.dumps(row))

    # response = "{" + ", ".join(result) + "}"
    return json.dumps(result)


# =======================================================

@app.route('/id0/<interval>')
def id0(interval):
    # connection = pymysql.connect(host='108.61.186.24',
    #                              user='bitmex3536',
    #                              #  password='',
    #                              password='BitMex*95645636',
    #                              db='aws_lambda_bitmex',
    #                              charset='utf8',
    #                              cursorclass=pymysql.cursors.DictCursor)
    connection = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='',
                                 db='aws_lambda_bitmex',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:

            sql = "SELECT `id`, `timestamp`, `open`, `high`, `low`, `close`, `volume`, `num_3`, `num_3i`, " + \
                  "`num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`, `date` FROM (SELECT D.* FROM `downloaded_" + interval + "` D ORDER BY D.timestamp DESC LIMIT 0, 2000) `sub` ORDER BY `timestamp` ASC;"
            cursor.execute(sql)
            rows1 = cursor.fetchall()
    except:
        print(connection.cursor()._last_executed)
        connection.close()
        abort(401)

    date_arr = []
    open_arr = []

    result_cnt = len(rows1) - 1
    for r in rows1:
        date_arr.append(r['date'])
        open_arr.append(r['open'])

    for r in range(0, 60):
        date_arr.append(rows1[result_cnt]['date'])
        open_arr.append(rows1[result_cnt]['open'])

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

    result = {
        "id": 0,
        "timestamp": rows1[result_cnt]['timestamp'],
        "open": rows1[result_cnt]['open'],
        "high": rows1[result_cnt]['high'],
        "low": rows1[result_cnt]['low'],
        "close": rows1[result_cnt]['close'],
        "volume": rows1[result_cnt]['volume'],
        'num_3': num_3[result_cnt].real,
        'num_3i': num_3[result_cnt].imag,
        'num_6': num_6[result_cnt].real,
        'num_6i': num_6[result_cnt].imag,
        'num_9': num_9[result_cnt].real,
        'num_9i': num_9[result_cnt].imag,
        'num_100': num_100[result_cnt].real,
        'num_100i': num_100[result_cnt].imag,
        "date": rows1[result_cnt]['date'],
    }
    # result["result"] = rows
    # for row in rows:
    #     result.append(json.dumps(row))

    # response = "{" + ", ".join(result) + "}"
    return json.dumps(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
    # app.run(host='0.0.0.0', port=443, ssl_context='adhoc')

