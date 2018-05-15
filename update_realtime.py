#coding=utf-8
from datetime import datetime, timedelta

import pandas as pd
import calendar
import os
import time
import dateutil
import pytz
import json

from utils import Util
from utils import WeatherData
import logging

DATE = datetime.now().strftime("%Y-%m-%d %H:%M")
# create a log file
logger = logging.getLogger('/home/shuailong/update_realtime/update_weather.log')
logger.setLevel(logging.DEBUG)
# create a handler, write the log info into it
fh = logging.FileHandler('/home/shuailong/update_realtime/update_weather.log')
fh.setLevel(logging.DEBUG)
# create another handler output the log though console
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# 定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# 给logger添加handler
logger.addHandler(fh)
logger.addHandler(ch)
# 记录一条日志
logger.info('%s  update file' % DATE)

CITY_DICT = {
    "NJ":{"name":"nanjing","influxKey": "TS526","gps":[31.7362530000,118.8832500000],"env_city_code":101190104,"wunder_gps":"/q/zmw:00000.862.58238"},
    "GZ":{"name":"guangzhou","influxKey": "TS1077","gps":[23.1751440000,113.4737990000],"env_city_code":101280101,"wunder_gps":"/q/zmw:00000.311.59287"}
}

city_dict = CITY_DICT["GZ"]
def update_dark_data(city_dict,temp_df):

    start_dt = temp_df.index[-1] +timedelta(hours=1)
    end_dt   = Util.tz.localize(datetime.now())
    if start_dt > end_dt:return temp_df
    WD = WeatherData()

    his_env_add  = WD.get_dark_weather(city_dict["gps"], start_dt, end_dt)    
    
    return pd.concat([temp_df.dropna()[:start_dt.strftime("%Y/%m/%d")],his_env_add[his_env_add.index < datetime.now()]])

temp_df = Util.read_csv("/home/shuailong/update_realtime/GZ_his_dark.csv")
GZ_his_dark = update_dark_data(city_dict, temp_df)
logger.info("before update history weather:" +str(temp_df.index[-1]))
logger.info("after update history weather:" +str(GZ_his_dark.index[-1]))

WD = WeatherData()
start_dt   = Util.tz.localize(datetime.now())
end_dt = start_dt + timedelta(days = 2)
GZ_dark_predict  = WD.get_dark_weather(CITY_DICT["GZ"]["gps"], start_dt, end_dt)
logger.info(str(GZ_dark_predict.index[0])+"        "+str(GZ_dark_predict.index[-1]))

GZ_his_dark.to_csv("/home/shuailong/update_realtime/GZ_his_dark.csv")
GZ_dark_predict.to_csv("/home/shuailong/update_realtime/predict_files/GZ_pred_dark_"+start_dt.strftime("%Y-%m-%d")+".csv")
