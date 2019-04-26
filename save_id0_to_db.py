import json
import sched
import time
from threading import Thread

import pymysql
import requests


def save_id0_to_db(interval):
    try:
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
        with connection.cursor() as cursor:
            # Create a new record
            sql = "CREATE TABLE IF NOT EXISTS `id0_{}` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,  `timestamp` varchar(765) DEFAULT NULL,  `open` double DEFAULT 0,  `high` double DEFAULT 0,  `low` double DEFAULT 0,  `close` double DEFAULT 0,  `volume` double DEFAULT 0,  `num_3` double DEFAULT 0,  `num_3i` double DEFAULT 0,  `num_6` double DEFAULT 0,  `num_6i` double DEFAULT 0,  `num_9` double DEFAULT 0,  `num_9i` double DEFAULT 0,  `num_100` double DEFAULT 0,  `num_100i` double DEFAULT 0,  PRIMARY KEY (`id`)) DEFAULT CHARSET=utf8;".format(
                interval)
            print(sql)
            cursor.execute(sql, None)
            connection.commit()

            url = "http://127.0.0.1/id0/{}".format(interval)

            r = requests.get(url=url)

            formatted_string = r.text.replace("'", '"')
            row = json.loads(formatted_string)

            sql = "SELECT `id` FROM `id0_{}` WHERE `timestamp` = '{}';".format(interval, row['timestamp'])
            print(sql)
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                sql = "INSERT INTO `id0_{}`(`timestamp`, `open`, `high`, `low`, `close`, `volume`, `num_3`, `num_3i`, `num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`) " + \
                      "VALUES('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');"
                sql = sql.format(interval, row['timestamp'], row['open'], row['high'], row['low'], row['close'],
                                 row['volume'], row['num_3'], row['num_3i'], row['num_6'], row['num_6i'],
                                 row['num_9'], row['num_9i'], row['num_100'], row['num_100i'])
                print(sql)
                cursor.execute(sql, None)
                connection.commit()
        result = {'result': 'ok'}
    except:
        result = {'result': 'fail'}

    return result


s = sched.scheduler(time.time, time.sleep)


def update_id0_table(sc):
    try:
        print('save_id0_to_db', save_id0_to_db('5m'))
        print('save_id0_to_db', save_id0_to_db('1h'))
        result = {'result': 'ok'}
    except:
        result = {'result': 'fail'}

    print('update_id0_table', result)
    s.enter(5 * 60, 1, update_id0_table, (sc,))
    return result


if __name__ == '__main__':
    s.enter(0, 1, update_id0_table, (s,))
    s.run()

