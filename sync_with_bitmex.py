import datetime
import json
import sched
import time

import numpy as np
import pandas as pd
import pymysql.cursors
import requests


def date_parser(x):
    return datetime.datetime.strptime(x, '%Y-%m-%d')


def float_parser(x):
    res = 0
    try:
        res = float(x)
    except:
        res = 0

    return res


def lambda_handler(event, context, interval):
    connection = pymysql.connect(host='108.61.186.24',
                                 user='bitmex3536',
                                 #  password='',
                                 password='BitMex*95645636',
                                 db='aws_lambda_bitmex',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    # connection = pymysql.connect(host='127.0.0.1',
    #                              user='root',
    #                              password='',
    #                              db='aws_lambda_bitmex',
    #                              charset='utf8',
    #                              cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "CREATE TABLE IF NOT EXISTS `downloaded_" + interval + "`(`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `date` varchar(255) DEFAULT NULL, `timestamp` varchar(255) DEFAULT NULL, `open` double DEFAULT NULL, `high` double DEFAULT NULL, `low` double DEFAULT NULL, `close` double DEFAULT NULL, `volume` double DEFAULT NULL, `num_3` double DEFAULT NULL, `num_3i` double DEFAULT NULL, `num_6` double DEFAULT NULL, `num_6i` double DEFAULT NULL, `num_9` double DEFAULT NULL, `num_9i` double DEFAULT NULL, `num_100` double DEFAULT NULL, `num_100i` double DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
            cursor.execute(sql, None)

            connection.commit()

            sql = "SELECT `timestamp` FROM `downloaded_" + interval + "` ORDER BY `timestamp` DESC LIMIT 0, 1;"
            cursor.execute(sql, None)

            rows = cursor.fetchall()
            if len(rows) > 0:
                timestamp = rows[0]['timestamp']
                timestamp = timestamp.replace('.000Z', '.100Z')
            else:
                timestamp = '2019-01-01T00:00:00.000Z'
    except:
        # print(connection.cursor()._last_executed)
        connection.close()
        return {
            'statusCode': 500
        }

    # Connect to the database
    url = "https://www.bitmex.com/api/v1/trade/bucketed"

    # defining a params dict for the parameters to be sent to the API
    params = {
        'binSize': interval,
        'partial': 'false',
        'symbol': 'XBTUSD',  ####### ETHUSD will calculate Eth
        'count': 750,
        'reverse': 'false',
        'startTime': timestamp,
    }

    # sending get request and saving the response as response object
    r = requests.get(url=url, params=params)

    try:
        formatted_string = r.text.replace("'", '"')
        rows = json.loads(formatted_string)
    except:
        return {
            'statusCode': 200,
            'body': 'ok',
            'formatted_string': formatted_string
        }

    print(url, rows)
    r_cnt = len(rows)
    if r_cnt == 0:
        return {
            'statusCode': 200,
            'body': 'ok',
        }

    try:
        with connection.cursor() as cursor:
            # Create a new record
            for r in range(0, r_cnt):
                sql = "INSERT INTO `downloaded_" + interval + "`(`date`, `timestamp`, `open`, `high`, `low`, `close`, `volume`) " + \
                      "VALUES (%s, %s, %s, %s, %s, %s, %s);"
                cursor.execute(sql, (
                    rows[r]['timestamp'][:10], rows[r]['timestamp'], float_parser(rows[r]['open']),
                    float_parser(rows[r]['high']),
                    float_parser(rows[r]['low']), float_parser(rows[r]['close']), float_parser(rows[r]['volume'])))

            connection.commit()

            sql = "SELECT `id`, `date`, `open` FROM (SELECT D.id, D.date, D.open, D.timestamp FROM `downloaded_" + interval + "` D ORDER BY D.timestamp DESC LIMIT 0, 2000) `sub` ORDER BY `timestamp` ASC;"
            cursor.execute(sql)
            rows = cursor.fetchall()

            id_arr = []
            date_arr = []
            open_arr = []

            result_cnt = len(rows)
            for r in rows:
                id_arr.append(r['id'])
                date_arr.append(r['date'])
                open_arr.append(r['open'])

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
            # print(num_3)
            # print(num_6)
            # print(num_9)
            # print(num_100)

            for r in range(0, result_cnt):
                # print(id_arr[r])
                sql = "UPDATE `downloaded_" + interval + "` SET `num_3` = %s, `num_3i` = %s, " + \
                      "`num_6` = %s, `num_6i` = %s, " + \
                      "`num_9` = %s, `num_9i` = %s, " + \
                      "`num_100` = %s, `num_100i` = %s WHERE `id` = '%s';"
                sql = cursor.mogrify(sql, (
                    float_parser(num_3[r].real), float_parser(num_3[r].imag),
                    float_parser(num_6[r].real), float_parser(num_6[r].imag),
                    float_parser(num_9[r].real), float_parser(num_9[r].imag),
                    float_parser(num_100[r].real), float_parser(num_100[r].imag),
                    float_parser(id_arr[r])
                ))
                # print(sql)
                cursor.execute(sql)

            connection.commit()
    except:
        print(connection.cursor()._last_executed)
        connection.close()
        return {
            'statusCode': 500
        }

    connection.close()

    return {
        'statusCode': 200,
        'body': 'ok',
    }


# def update():
#     print(lambda_handler(None, None, '1m'))
#     print(lambda_handler(None, None, '5m'))
#     print(lambda_handler(None, None, '1h'))


s = sched.scheduler(time.time, time.sleep)


def do_something(sc):
    print("Doing sync...")
    print(str(datetime.datetime.now()))
    try:
        print(lambda_handler(None, None, '1m'))
        print(lambda_handler(None, None, '5m'))
        print(lambda_handler(None, None, '1h'))
    except:
        time.sleep(5 * 60)
    # do your stuff
    s.enter(60, 1, do_something, (sc,))


s.enter(0, 1, do_something, (s,))
s.run()
# while True:
#     update()
#     time.sleep(10)
