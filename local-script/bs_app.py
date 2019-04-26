import json
import sched
import time

import requests
import server_config


def bs_func():
    url = "{}/bs_with_bybit".format(server_config.server_url)

    r = requests.post(url=url)

    formatted_string = r.text.replace("'", '"')
    rows = json.loads(formatted_string)
    return rows


s = sched.scheduler(time.time, time.sleep)


def update_id0_table(sc):
    try:
        print('bs_func', bs_func())
        result = {'result': 'ok'}
    except:
        result = {'result': 'fail'}

    print('update_id0_table', result)
    s.enter(5 * 60, 1, update_id0_table, (sc,))
    return result


if __name__ == '__main__':
    s.enter(0, 1, update_id0_table, (s,))
    s.run()