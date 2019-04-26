import datetime
import json
from datetime import timedelta

import numpy as np
import pandas as pd
import pymysql.cursors
import pymysql.cursors
import requests
from flask import Flask, abort, request

import mysql_config
from bybit import bybit_api

app = Flask(__name__)


def date_parser(x):
    return datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.000Z')


@app.route('/get_custom_bitmex/<interval>')
def get_custom_bitmex(interval):
    connection = pymysql.connect(host=mysql_config.host,
                                 user=mysql_config.user,
                                 #  password='',
                                 password=mysql_config.password,
                                 db=mysql_config.db,
                                 charset=mysql_config.charset,
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
    connection = pymysql.connect(host=mysql_config.host,
                                 user=mysql_config.user,
                                 #  password='',
                                 password=mysql_config.password,
                                 db=mysql_config.db,
                                 charset=mysql_config.charset,
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


# =======================================================

@app.route('/id0_collection/<interval>')
def id0_collection(interval):
    params = request.args
    mode = params.get('mode')
    try:
        connection = pymysql.connect(host=mysql_config.host,
                                     user=mysql_config.user,
                                     #  password='',
                                     password=mysql_config.password,
                                     db=mysql_config.db,
                                     charset=mysql_config.charset,
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            if mode == 'all':
                sql = "SELECT `timestamp`, `open`, `high`, `low`, `close`, `volume`, `num_3`, `num_3i`, " +\
                      "`num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i` FROM `id0_{}` ORDER BY `timestamp` " +\
                      "ASC;"
            else:
                sql = "SELECT I.* FROM (SELECT `timestamp`, `open`, `high`, `low`, `close`, `volume`, `num_3`, `num_3i`, " +\
                      "`num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i` FROM `id0_{}` ORDER BY `timestamp` " +\
                      "DESC LIMIT 0, 2000) `I` ORDER BY I.timestamp ASC;"
            sql = sql.format(interval)
            print(sql)
            cursor.execute(sql)
            rows1 = cursor.fetchall()
    except:
        connection.close()
        abort(401)

    rows = []
    cnt = len(rows1) - 1
    idx = 0
    for row in rows1:
        rows.append(row)
        rows[idx]['id'] = cnt - idx
        idx = idx + 1

    # last_timestamp = date_parser(rows[cnt]['timestamp'])
    # # print(last_timestamp)
    # for i in range(0, 60):
    #     if interval == "1m":
    #         last_timestamp = last_timestamp + timedelta(minutes=1)
    #     elif interval == "5m":
    #         last_timestamp = last_timestamp + timedelta(minutes=5)
    #     elif interval == "1h":
    #         last_timestamp = last_timestamp + timedelta(hours=1)
    #
    #     row1 = {
    #         'id': cnt - idx,
    #         'timestamp': last_timestamp.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
    #         'open': rows[cnt]['open'],
    #         'high': rows[cnt]['high'],
    #         'low': rows[cnt]['low'],
    #         'close': rows[cnt]['close'],
    #         'volume': rows[cnt]['volume'],
    #         'num_3': rows[cnt]['num_3'],
    #         'num_3i': rows[cnt]['num_3i'],
    #         'num_6': rows[cnt]['num_6'],
    #         'num_6i': rows[cnt]['num_6i'],
    #         'num_9': rows[cnt]['num_9'],
    #         'num_9i': rows[cnt]['num_9i'],
    #         'num_100': rows[cnt]['num_100'],
    #         'num_100i': rows[cnt]['num_100i'],
    #     }
    #     rows.append(row1)
    #     idx = idx + 1

    result = {
        "result": rows
    }
    # result["result"] = rows
    # for row in rows:
    #     result.append(json.dumps(row))

    # response = "{" + ", ".join(result) + "}"
    return json.dumps(result)


# =======================================================

@app.route('/bs_with_bybit', methods=['POST'])
def bs_with_bybit():
    remote_addr = request.remote_addr
    print('ip', remote_addr)
    if remote_addr != '127.0.0.1':
        return json.dumps({
            'result': 'bad',
            'error': 'ip invalid',
            'ip': remote_addr,
        })
    try:
        result_cancel = ''
        connection = pymysql.connect(host=mysql_config.host,
                                     user=mysql_config.user,
                                     #  password='',
                                     password=mysql_config.password,
                                     db=mysql_config.db,
                                     charset=mysql_config.charset,
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            sql = "SELECT * FROM last_order_id O ORDER BY O.timestamp DESC LIMIT 0, 1;"
            cursor.execute(sql)
            rows = cursor.fetchall()
            last_order_id = None
            if len(rows) > 0:
                last_order_id = rows[0]['order_id']
            if last_order_id is not None:
                order_list = bybit_api.order_list(last_order_id)
                order_data = order_list['result']['data']
                # return json.dumps(order_data)
                if len(order_data) > 0:
                    result_cancel = bybit_api.order_cancel(order_data[0]['order_id'])
                # return json.dumps(order_data)

            position_list = bybit_api.position_list()
            print('position_list', position_list)
            positions = position_list['result']
            print('positions', positions)
            position_btcusd = None
            for p in positions:
                if p['symbol'] == 'BTCUSD':
                    position_btcusd = p
                    # break

            print('position_btcusd', position_btcusd)
            if position_btcusd is not None:
                url = "http://127.0.0.1/id0/5m"

                r = requests.get(url=url)

                formatted_string = r.text.replace("'", '"')
                id0 = json.loads(formatted_string)
                print('id0', id0)
                price = int(1.1 * id0['num_100'])
                print('price', price)
                if position_btcusd['size'] > 0:
                    result_btcusd = bybit_api.order_create(order_type='Limit', symbol='BTCUSD', side='Sell',
                                                           qty=position_btcusd['size'], price=price,
                                                           time_in_force='GoodTillCancel')
                                                           # qty=1, price=price, time_in_force='GoodTillCancel')
                    if result_btcusd['ret_code'] == 0:
                        result1 = result_btcusd['result']
                        sql = "INSERT INTO `last_order_id`(`order_id`, `side`, `state`, `size`, `price`, `timestamp`) " +\
                              "VALUES('{}', '{}', '{}', '{}', '{}', '{}');" \
                                  .format(result1['order_id'], 'Sell', result1['order_status'],
                                          position_btcusd['size'], price, result_btcusd['time_now'])
                        cursor.execute(sql, None)
                        connection.commit()
                else:
                    result_btcusd = bybit_api.order_create(order_type='Limit', symbol='BTCUSD', side='Buy',
                                                           qty=1, price=price, time_in_force='GoodTillCancel')
                    if result_btcusd['ret_code'] == 0:
                        result1 = result_btcusd['result']
                        sql = "INSERT INTO `last_order_id`(`order_id`, `side`, `state`, `size`, `price`, `timestamp`) " + \
                              "VALUES('{}', '{}', '{}', '{}', '{}', '{}');" \
                                  .format(result1['order_id'], 'Buy', result1['order_status'],
                                          position_btcusd['size'], price, result_btcusd['time_now'])
                        cursor.execute(sql, None)
                        connection.commit()
    except:
        connection.close()
        abort(401)

    response = {
        'result': 'ok',
        'old': order_data,
        'new': result_btcusd}
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
    # app.run(host='0.0.0.0', port=443, ssl_context='adhoc')

