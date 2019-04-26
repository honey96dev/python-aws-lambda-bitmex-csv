from bitmex_requests import writeJson;
from datetime import datetime

start = datetime(2015, 10, 1, 0, 0);      ### Data starts 2015-10-1, there is no data prior to this day
end = datetime(2015, 11, 2, 0, 0)
cols = ["open", "close", "high", "low"];

writeJson("XBTUSD", start, end, "30m", cols,"BTC_minutes.txt");
