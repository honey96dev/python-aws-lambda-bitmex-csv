import requests
import json
import csv
import pymysql.cursors


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
            sql = "CREATE TABLE IF NOT EXISTS `downloaded`(`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `timestamp` varchar(255) DEFAULT NULL, `symbol` varchar(255) DEFAULT NULL, `open` double DEFAULT NULL, `high` double DEFAULT NULL, `low` double DEFAULT NULL, `close` double DEFAULT NULL, `trades` double DEFAULT NULL, `volume` double DEFAULT NULL, `vwap` double DEFAULT NULL, `lastSize` double DEFAULT NULL, `turnover` double DEFAULT NULL, `homeNotional` double DEFAULT NULL, `foreignNotional` double DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
            cursor.execute(sql, None)

        connection.commit()
    except:
        return {
            'statusCode': 500
        }

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "CREATE TABLE IF NOT EXISTS `downloaded`(`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `timestamp` varchar(255) DEFAULT NULL, `symbol` varchar(255) DEFAULT NULL, `open` double DEFAULT NULL, `high` double DEFAULT NULL, `low` double DEFAULT NULL, `close` double DEFAULT NULL, `trades` double DEFAULT NULL, `volume` double DEFAULT NULL, `vwap` double DEFAULT NULL, `lastSize` double DEFAULT NULL, `turnover` double DEFAULT NULL, `homeNotional` double DEFAULT NULL, `foreignNotional` double DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
            cursor.execute(sql, None)

        connection.commit()
    except:
        return {
            'statusCode': 500
        }

    url = "https://www.bitmex.com/api/v1/trade/bucketed"

    # defining a params dict for the parameters to be sent to the API
    params = {
        'binSize': '5m',
