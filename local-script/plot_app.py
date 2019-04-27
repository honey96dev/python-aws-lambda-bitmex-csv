import json

import matplotlib.pyplot as plt
import requests
import server_config


def draw_plot(interval):
    try:
        url = "{}/id0_collection/{}".format(server_config.server_url, interval)

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
    fig, ax1 = plt.subplots()
    ax1.set_ylabel('Price')
    ax1.plot(timestamp, open_price, 'C0', label='open')
    ax1.plot(timestamp, num_3, 'C1', label='num_3')
    ax1.plot(timestamp, num_6, 'C3', label='num_6')
    ax1.plot(timestamp, num_9, 'C5', label='num_9')
    ax1.plot(timestamp, num_100, 'C7', label='num_100')

    ax2 = ax1.twinx()
    ax2.set_ylabel('Imagine')
    ax2.plot(timestamp, num_3i, 'C2:', label='num_3i')
    ax2.plot(timestamp, num_6i, 'C4:', label='num_6i')
    ax2.plot(timestamp, num_9i, 'C6:', label='num_9i')
    ax2.plot(timestamp, num_100i, 'C8:', label='num_100i')
    frame1 = fig.gca()
    frame1.axes.get_xaxis().set_ticks(dates)
    plt.title('Bitmex-id0({})'.format(interval))
    plt.xlabel('Timestamp')
    ax1.legend()
    ax2.legend()
    fig.tight_layout()
    plt.show()
    result = {'result': 'ok'}
    return result


if __name__ == '__main__':
    print(draw_plot('5m'))
    print(draw_plot('1h'))
