# import requests
import json
import pymysql.cursors


def lambda_handler(event, context, interval):
    connection = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='',
                                 # password='skdmlMysql@123456',
                                 db='aws_lambda_bitmex',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:

            sql = "SELECT `id`, `date`, `timestamp`, `open` FROM (SELECT D.id, D.date, D.open, D.timestamp FROM `downloaded_" + interval + "` D ORDER BY D.timestamp DESC LIMIT 0, 2000) `sub` ORDER BY `timestamp` ASC;"
            cursor.execute(sql)
            rows = cursor.fetchall()
    except:
        print(connection.cursor()._last_executed)
        connection.close()
        return {
            'statusCode': 500
        }

    # rows = []
    # for row in rows:
    #     rows.append(json.dumps(row))

    # print(json.dumps(rows))
    return {
        'statusCode': 200,
        'body': json.dumps(rows)
    }


print(lambda_handler(None, None, '5m'))
print(lambda_handler(None, None, '1h'))
#
# a = [
#      ]
