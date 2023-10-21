# -*- coding: utf-8 -*-
from binance.um_futures import UMFutures
from binance_api import key,secret
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime
import time
import logging
from binance.error import ClientError

class BotTemplate:
    def __init__(self):
        self.connect_api(key = key,secret = secret)    
        self.printlog = True
        self.orderId = None

    def connect_api(self,key,secret):# 登录代码
        self.client = UMFutures(key=key,
                           secret=secret,
                           timeout=1,) 
        
    def log(self, txt):#打印日志
        if self.printlog:
            current_timestamp = time.time()
            local_time = time.localtime(current_timestamp)
            current_time = time.strftime( "%Y-%m-%d %H:%M:%S", local_time)
            log_line = '%s, %s' % (current_time, txt)
            print(log_line)
            with open('Strategy_log.txt', 'a') as f:
                f.write(log_line + '\n')    
                
    def get_continuousklines(self,symbol,timeframe,limit):# 获取K线数据的代码
        # https://binance-docs.github.io/apidocs/futures/en/#continuous-contract-kline-candlestick-data
        ohlcv = self.client.continuous_klines(symbol, "PERPETUAL",timeframe,limit = limit)
        #移除未形成K线
        ohlcv.pop()
        #kdf = pd.DataFrame(ohlcv, columns=["opentime","open","high","low","close","volume","closetime","avolume","numtrade","takerbuy","takerbuyavolume","ign"])
        kdf = pd.DataFrame(ohlcv)
        kdf = kdf.iloc[:, 0:6]
        kdf.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        kdf.open = kdf.open.astype("float")
        kdf.high = kdf.high.astype("float")
        kdf.low = kdf.low.astype("float")
        kdf.close = kdf.close.astype("float")
        kdf.volume = kdf.volume.astype("float")
        kdf.datetime = [datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.datetime]  # update datetime format
        kdf.set_index('datetime', inplace=True)  # set datetime as index

        return kdf
    
    def market_buy(self,symbol,amount): # 下市价买单
        try:
            self.client.new_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=amount,
            timeInForce="GTC",)
            
        except ClientError as error:
            logging.error("Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message))
        

    def market_sell(self,symbol,amount): # 下市价卖单
        try:
            self.client.new_order(
            symbol=symbol,
            side="SELL",
            type="MARKET",
            quantity=amount,
            timeInForce="GTC",)
            
        except ClientError as error:
            logging.error("Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message))
        

    def limit_buy(self,symbol, amount, price): # 下限价买单
        try:
            self.client.new_order(
            symbol=symbol,
            side="BUY",
            type="LIMIT",
            quantity=amount,
            timeInForce="GTC",
            price=price)
            
        except ClientError as error:
            logging.error("Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message))
        
    
    def limit_sell(self,symbol, amount, price): # 下限价卖单
        try:
            self.client.new_order(
            symbol=symbol,
            side="SELL",
            type="LIMIT",
            quantity=amount,
            timeInForce="GTC",
            price=price)
            
        except ClientError as error:
            logging.error("Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message))
        

    def fetch_last_order(self,symbol):
        orders = self.client.get_all_orders(symbol=symbol, recvWindow=2000)
        last_order = orders[-1]
        return last_order

    def cancel_order(self,symbol,orderId):
        try:
            self.client.cancel_order(symbol=symbol, orderId=orderId, recvWindow=2000)
            
        except ClientError as error:
            logging.error("Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message))   

    def get_trades(self,symbol,dt,days):#获取交易数据，分为买单集和卖单集
        trades = self.client.get_account_trades(symbol=symbol, recvWindow=6000)
        ts = int(dt.timestamp())*1000
        trade_buy = []
        trade_sell= []
        for t in trades:
            time = int(t["info"]["time"])
            if time >= ts and time <= ts + 86400000*days:
                time = datetime.fromtimestamp(time/1000)
                side = t["info"]["side"]
                price = float(t["info"]["price"])
                if side == "BUY":
                    trade_buy.append([time,price])
                else:
                    trade_sell.append([time,price])
            
        trade_buy = pd.DataFrame(trade_buy)
        trade_sell = pd.DataFrame(trade_sell)
        trade_buy.columns = ["time","price"]
        trade_sell.columns = ["time","price"]
        return trade_buy, trade_sell
    
class Indicators:
    def macd(kdf,fast,slow,signal):
        ema_fast = kdf['close'].ewm(span=fast, adjust=False).mean() 
        ema_slow = kdf['close'].ewm(span=slow, adjust=False).mean()
        macd = pd.concat([ema_fast,ema_slow], axis=1)
        macd.columns = ["ema_fast","ema_slow"]
        macd.loc[:,'diff'] = macd["ema_fast"] - macd["ema_slow"]
        macd.loc[:,'dea'] = macd['diff'].ewm(span=signal, adjust=False).mean()
        macd.loc[:,'macd'] = macd['diff'] - macd['dea']
        # 生成金叉信号
        condition1 = macd['diff'] > macd['dea']  
        condition2 =  macd['diff'].shift(1) <=  macd['dea'].shift(1)
        macd['GXvalue'] = np.where(condition1 & condition2, macd['dea'], 0) 
        # 生成死叉信号
        condition3 = macd['diff'] < macd['dea']  
        condition4 = macd['diff'].shift(1) >=  macd['dea'].shift(1)
        macd['DXvalue'] = np.where(condition3 & condition4, macd['dea'], 0)
        return macd
    
    def dema(kdf,period):
        ema = kdf['close'].ewm(span=period, adjust=False).mean()
        ema2 = ema.ewm(span=period, adjust=False).mean()
        dema = pd.concat([ema,ema2], axis=1)
        dema.columns = ["ema","dema"]
        return dema
    
    def adx(kdf,period):
        adx = ta.adx(kdf['high'], kdf['low'], kdf['close'], length=period)
        adx.columns = ["adx","plus","minus"]
        return adx

    def stochrsi(kdf,period,kd):
        stochrsi = ta.stochrsi(kdf['close'],rsi_length=period,k=kd,d=kd)
        stochrsi.columns = ["k","d"]
        # 生成金叉信号
        condition1 = stochrsi['k'] > stochrsi['d']  
        condition2 =  stochrsi['k'].shift(1) <=  stochrsi['d'].shift(1)
        stochrsi['GXvalue'] = np.where(condition1 & condition2, stochrsi['d'], 0) 
        # 生成死叉信号
        condition3 = stochrsi['k'] < stochrsi['d']  
        condition4 = stochrsi['k'].shift(1) >=  stochrsi['d'].shift(1)
        stochrsi['DXvalue'] = np.where(condition3 & condition4, stochrsi['d'], 0)
        return stochrsi
    
    def engulfing(kdf):
        kdf["body"] = kdf["close"] - kdf["open"]
        # 前一根阳线
        condition1 = kdf["body"].shift(1) > 0 
        # 后一根阴线吞没
        condition2 = kdf["close"] < kdf["low"].shift(1)
        # 前一根阴线
        condition3 = kdf["body"].shift(1) < 0 
        # 后一根阳线吞没
        condition4 = kdf["close"] > kdf["high"].shift(1)
        kdf['engulfing'] = np.where(condition1 & condition2, -1,
                                    np.where(condition3 & condition4, 1, 0))
    
    def hammer(kdf,hammer_k):
        kdf["body"] = kdf["close"] - kdf["open"]
        kdf["upwick"] = np.where(kdf["body"] >0, kdf["high"] - kdf["close"],kdf["high"]-kdf["open"])
        kdf["downwick"] = np.where(kdf["body"] >0, kdf["open"] - kdf["low"],kdf["close"] - kdf["low"])
        # 前一根阳线
        condition1 = kdf["body"].shift(1) > 0 
        # 后一根阴线倒锤且柄大于锤头K倍
        condition2 = kdf["body"] < 0 
        condition3 = kdf["upwick"] >= abs(kdf["body"])*hammer_k
        # 前一根阴线
        condition4 = kdf["body"].shift(1) < 0 
        # 后一根阳线锤子且柄大于锤头K倍
        condition5 = kdf["body"] > 0 
        condition6 = kdf["downwick"] >=  abs(kdf["body"])*hammer_k
        kdf['hammer'] = np.where(condition1 & condition2 & condition3, -1,
                                    np.where(condition4 & condition5 & condition6, 1, 0))