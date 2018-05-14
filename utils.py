#coding=utf-8
from __future__ import unicode_literals
from __future__ import division
from scipy.signal import savgol_filter
from datetime import datetime, timedelta

import pandas as pd
import calendar
import os
import time
import dateutil
import influxdb
import pytz
import json
import requests
from requests.auth import HTTPDigestAuth

import urllib
import urllib2


class Util(object):
    tz = pytz.timezone("Asia/Shanghai")

    @staticmethod
    def dt_to_ts(dt):

        if not dt.tzinfo:
            dt = Util.tz.localize(dt)

        return calendar.timegm(dt.utctimetuple())

    def ts_to_dt(self, timestamp):

        return datetime.fromtimestamp(timestamp, self.tz)
    
    def str_to_ts(self, timeStr, timeZoneStr=" +8"):
        # set timezone
        timeStr = timeStr + timeZoneStr
        # convert datetime format
        parsedStr = dateutil.parser.parse(timeStr)
        # return utc timestamp
        return calendar.timegm(parsedStr.utctimetuple())

    def str_to_dt(self, timeStr):

        dt = self.str_to_ts(timeStr)

        return self.ts_to_dt(dt)

    @staticmethod
    def read_csv(fname):

        raw_df = pd.read_csv(fname)
        raw_df.columns = ["time"]+list(raw_df.columns)[1:]
        index  = pd.to_datetime(raw_df["time"], utc=True)
        raw_df.index   = index

        df = raw_df.drop(["time"], axis=1)
        df.index = df.index.tz_convert("Asia/Shanghai")

        return df

    @staticmethod
    def read_json(fname):
        with open(fname, "rb") as r:
            return json.load(r)

    @staticmethod
    def write_json(fname, data):
        with open(fname, 'wb') as w:
            json.dump(data, w)

    @staticmethod
    def save_data(influx_keys, client, start="'2016-12-31 16:00:00'"):

        for key in influx_keys:
            sql = "select * from %s where time > %s" % (key, start)
            print sql
            df  = dict(client.query(sql))[key]
            sql = "select * from %s order by time desc limit 1" % key
            print dict(client.query(sql))[key].index[0]
            fname = os.path.join("test", "%s.csv" % key)
            df.to_csv(fname)
            time.sleep(2)


class WeatherData(object):

    def __init__(self):
        pass

    def get_env_weather(self, city_ID, start_dt, end_dt):
        url = "http://service.envicloud.cn:8082/v2/weatherhistory/BGVVBMFYZG1HYZE1MTU5ODYXNZIYMJC=/%s/%s%s%s/%s"
        hum = []
        tem = []
        con = []
        times = []

        index = pd.date_range(start_dt, end_dt, freq="3600s")[:-1]

        while start_dt < end_dt:
            year  = start_dt.strftime("%Y")
            month = start_dt.strftime("%m")
            day   = start_dt.strftime("%d")
            hour  = start_dt.strftime("%H")
            req = url % (str(city_ID), year, month, day, hour)
            print req
            response = requests.request("GET", req)
            data = json.loads(response.text)
            start_dt = start_dt + timedelta(hours = 1)

            if not set(["humidity","temperature"]).issubset(data.keys()):continue

            if float(data["temperature"]) > 100.0:continue
            tem.append(data["temperature"])
            hum.append(data["humidity"])
            times.append(data["updatetime"])
            time.sleep(1)

        env_temp = pd.to_numeric(pd.Series(tem))
        env_hum  = pd.to_numeric(pd.Series(hum))
        time_index = [Util().str_to_dt(i) for i in times]
        weather = pd.concat([env_hum, env_temp],axis = 1)
        weather.index = time_index
        weather.columns = ["Humidity","TemperatureC"]

        return weather

    def get_dark_weather(self, gps, start, end):
        time = []
        temp = []
        hum  = []
        con  = []
        url = "https://api.darksky.net/forecast/f833983a2a0b6bf2d767a3394b21f636/%s,%s,%s?exclude=currently,flags"
        payload = ""
        headers = {
            'cache-control': "no-cache"
            }

        while start < end:
            
            ts = Util().dt_to_ts(start)
            turl = url % (gps[0], gps[1], str(ts))
            print turl
            response = requests.request("GET", turl, data=payload, headers=headers)
            data = json.loads(response.text)

            for i in data["hourly"]["data"]:
                time.append(Util().ts_to_dt(i["time"]))
                temp.append(round((float(i["temperature"])-32)*5/9,1))
                hum.append(i["humidity"]*100)
                con.append(i["summary"])

            start += timedelta(days=1)
        weather = pd.concat([pd.Series(temp),pd.Series(hum),pd.Series(con)],axis = 1)
        weather.index= pd.Series(time)
        weather.columns =["TemperatureC","Humidity","Conditions"]
        return weather

class UpdateWunder(object):
    def __init__(self):
        pass

    def update_wunder_data(self, influx_keys, client, start_dt, end_dt):

        start = Util().dt_to_ts(start_dt)
        end   = Util().dt_to_ts(end_dt)
        for key in influx_keys:
            sql = "select * from %s where time > %ss and time < %ss" % (key, start, end)
            print sql
            data  = dict(client.query(sql))
            try:
                df = data[key]
            except:
                print "%s not in data" % key
                return pd.DataFrame()
                
            sql = "select * from %s order by time desc limit 1" % key
            print dict(client.query(sql))[key].index[0]

            time.sleep(2)
        return df


if __name__ == "__main__":
    start  = Util.tz.localize(datetime(2018, 1, 1))
    end    = Util.tz.localize(datetime(2018, 1, 2))
