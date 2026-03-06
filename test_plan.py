import datetime
from zoneinfo import ZoneInfo
timezone = ZoneInfo('Asia/Kolkata')
print(datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S"))
