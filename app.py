from flask import Flask, abort
import json
import pymysql.cursors
from datetime import timedelta  
import datetime

app = Flask(__name__)

def date_parser(x):
    return datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.000Z')

@app.route('/get_custom_bitmex/<interval>')
def lambda_handler(interval):
    connection = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='',
                                #  password='skdmlMysql@123456',
                                 db='aws_lambda_bitmex',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:

            sql = "SELECT `id`, `timestamp`, `open`, `high`, `low`, `close`, `volume`, `num_3`, `num_3i`, " +\
                  "`num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i` FROM (SELECT D.* FROM `downloaded_" + interval + "` D ORDER BY D.timestamp DESC LIMIT 0, 2000) `sub` ORDER BY `timestamp` ASC;"
            cursor.execute(sql)
            rows1 = cursor.fetchall()
    except:
        print(connection.cursor()._last_executed)
        connection.close()
        abort(401)

    rows = []
    cnt = len(rows1) - 1
    idx = 0
    for row in rows1:
        rows.append(row)
        rows[idx]['id'] = cnt - idx
        idx = idx + 1
    
    last_timestamp = date_parser(rows[cnt]['timestamp'])
    print(last_timestamp)
    for i in range(0, 60):
        if interval == "5m":
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
            'num_3': rows[cnt]['num_3'],
            'num_3i': rows[cnt]['num_3i'],
            'num_6': rows[cnt]['num_6'],
            'num_6i': rows[cnt]['num_6i'],
            'num_9': rows[cnt]['num_9'],
            'num_9i': rows[cnt]['num_9i'],
            'num_100': rows[cnt]['num_100'],
            'num_100i': rows[cnt]['num_100i']
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=443, ssl_context='adhoc')
    # app.run(host='0.0.0.0', port=80)
