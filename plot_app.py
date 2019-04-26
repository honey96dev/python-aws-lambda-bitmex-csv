import json

import matplotlib.pyplot as plt
import requests
import pandas as pd


def draw_plot(interval):
    try:
        url = "http://127.0.0.1/id0_collection/{}".format(interval)

        r = requests.get(url=url)

        formatted_string = r.text.replace("'", '"')
        rows = json.loads(formatted_string)

    except:
        return {'result': 'http-error'}
    rows = rows['result']
    print(rows)
    # try:
    dates = []
    timestamp = []
    open_price = []
    num_3 = []
    num_3i = []
    num_6 = []
    num_6i = []
    num_9 = []
    num_9i = []
    num_100 = []
    num_100i = []

    cnt = 0
    for r in rows:
        cnt += 1
        if cnt % 100 is 0:
            dates.append(r['timestamp'][:16])
        else:
            dates.append('')
        timestamp.append(r['timestamp'])
        open_price.append(r['open'])
        num_3.append(r['num_3'])
        num_3i.append(r['num_3i'])
        num_6.append(r['num_6'])
        num_6i.append(r['num_6i'])
        num_9.append(r['num_9'])
        num_9i.append(r['num_9i'])
        num_100.append(r['num_100'])
        num_100i.append(r['num_100i'])
    # except:
    #     return {'result': 'json-error'}
    plt.figure(figsize=(16, 12))
    plt.plot(timestamp, open_price, 'C0', label='open')
    plt.plot(timestamp, num_3, 'C1', label='num_3')
    plt.plot(timestamp, num_3i, 'C2', label='num_3i')
    plt.plot(timestamp, num_6, 'C3', label='num_6')
    plt.plot(timestamp, num_6i, 'C4', label='num_6i')
    plt.plot(timestamp, num_9, 'C5', label='num_9')
    plt.plot(timestamp, num_9i, 'C6', label='num_9i')
    plt.plot(timestamp, num_100, 'C7', label='num_100')
    plt.plot(timestamp, num_100i, 'C8', label='num_100i')
    frame1 = plt.gca()
    frame1.axes.get_xaxis().set_ticks(dates)
    plt.title('Bitmex-id0({})'.format(interval))
    plt.ylabel('Price')
    plt.xlabel('Timestamp')
    plt.legend()
    plt.show()
    result = {'result': 'ok'}
    return result


if __name__ == '__main__':
    print(draw_plot('5m'))
    print(draw_plot('1h'))
