# -*- coding: utf-8 -*-

from botbase import BotTemplate,Indicators
import pandas as pd
import pandas_ta as ta
import warnings
warnings.filterwarnings("ignore")
import time 
import threading
import traceback

class kpattern_bot(BotTemplate):

    def __init__(self):
        super().__init__()
        
        self.vol_period = 33
        self.vol_k = 4
        self.dema_period = 19
        self.hammer_k = 3
        self.atrk = 3
        self.fixed_wlr = 1 # fixed win/loss ratio
        self.fixed_size = 0.01
        self.symbol = "ETHUSDT"

        self.index_signal = 0
        self.stopprice = 0
        self.stopprofit = 0

    def index_sig(self,kdf):
        dema = Indicators.dema(kdf,self.dema_period)
        kdf_sig = pd.concat([kdf,dema], axis=1)
        kdf_sig["atr"] =  ta.atr(kdf['high'], kdf['low'], kdf['close'], length=self.vol_period)
        kdf_sig["vol_ma"] = ta.ema(kdf["volume"],length = self.vol_period)
        Indicators.engulfing(kdf_sig)
        Indicators.hammer(kdf_sig,self.hammer_k)
        kdf_sig["signal"] = 0  
        # 低位反转
        kdf_sig.loc[(kdf_sig["close"]<kdf_sig["dema"]) & (kdf_sig["volume"]>= kdf_sig["vol_ma"] *self.vol_k) & (kdf_sig["engulfing"]==1), "signal"] = 1
        kdf_sig.loc[(kdf_sig["close"]<kdf_sig["dema"]) & (kdf_sig["volume"]>= kdf_sig["vol_ma"] *self.vol_k) & (kdf_sig["hammer"]==1), "signal"] = 1
        # 高位反转
        kdf_sig.loc[(kdf_sig["close"]>kdf_sig["dema"]) & (kdf_sig["volume"]>= kdf_sig["vol_ma"] *self.vol_k) & (kdf_sig["engulfing"]==-1), "signal"] = -1
        kdf_sig.loc[(kdf_sig["close"]>kdf_sig["dema"]) & (kdf_sig["volume"]>= kdf_sig["vol_ma"] *self.vol_k) & (kdf_sig["hammer"]==-1), "signal"] = -1

        self.log(f'body: {kdf_sig["body"][-1]:.2f}')
        self.log(f'upwick: {kdf_sig["upwick"][-1]:.2f}')
        self.log(f'downwick: {kdf_sig["downwick"][-1]:.2f}')
        self.log('....................................')
        self.log(f'Engulfing: {kdf_sig["engulfing"][-1]:.2f}')
        self.log(f'Hammer: {kdf_sig["hammer"][-1]:.2f}')
        self.log('....................................')
        self.log(f'Close: {kdf_sig["close"][-1]:.2f}')
        self.log(f'Dema: {kdf_sig["dema"][-1]:.2f}')
        self.log(f'Volume: {kdf_sig["volume"][-1]:.2f}')
        self.log(f'Vol_ma: {kdf_sig["vol_ma"][-1]:.2f}')
        self.log('....................................')
        self.log(f'Signal: {kdf_sig["signal"][-1]}')

        self.index_signal = kdf_sig
        self.log("------------------------------------------------")

    def manage_pos(self):
        #获取当前价格
        ticker = self.client.ticker_price(self.symbol)
        price = float(ticker['price'])
        #获取信号和波动空间sigma
        signal = self.index_signal["signal"][-1]
        sigma = self.index_signal["atr"][-1] * self.atrk
        #获取当前持仓信息
        positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
        position = positions.query('symbol == @self.symbol')
        positionAmt =  float(position['positionAmt'])
        entryPrice = float(position['entryPrice']) 
        #获取当前订单信息,自动补单
        last_order = self.fetch_last_order(self.symbol)
        self.orderId = last_order["orderId"]
        side = last_order["side"] 
        if last_order["status"] in ['NEW','PARTIALLY_FILLED']: 
            orderAmt = float(last_order['origQty'])
            if side =="BUY":
                self.cancel_order(self.symbol,self.orderId)
                self.order_Id = self.limit_buy(self.symbol, orderAmt, price)
            if side =="SELL":
                self.cancel_order(self.symbol,self.orderId)
                self.order_Id = self.limit_sell(self.symbol, orderAmt, price)
        elif last_order["status"] in ['FILLED','CANCELED']:
            #无仓位
            if positionAmt == 0:
                self.log("------------------------")
                if signal == 1:
                    self.log("++++++++++++Sendong Buy Order++++++++++++")
                    self.limit_buy(self.symbol,self.fixed_size,price)
                    self.stopprice = price-sigma
                    self.stopprofit = price + sigma*self.fixed_wlr
                elif signal == -1:
                    self.log("------------Sending Sell Order------------")
                    self.limit_sell(self.symbol,self.fixed_size,price)
                    self.stopprice = price+sigma
                    self.stopprofit = price - sigma*self.fixed_wlr
            #持有多仓
            if positionAmt > 0:
                self.log(f"Position Size: {positionAmt}") 
                if price > self.stopprofit or price <= self.stopprice:
                    self.order_Id = self.limit_sell(self.symbol,positionAmt,price)
                    self.log("------------Closing Long------------")
                elif signal == -1:
                     self.log("------------Reverse Long to Short ------------")
                     self.order_Id = self.limit_sell(self.symbol,2*self.fixed_size,price)
                     self.stopprice = price+sigma
                     self.stopprofit = price - sigma*self.fixed_wlr
                else:
                    self.log(f"Entry Price: {entryPrice}")
                    self.log(f"Stop Loss at : {self.stopprice:.2f}")
                    self.log(f"Stop Profit at : {self.stopprofit:.2f}")

            #持有空仓
            if positionAmt < 0:
                self.log(f"Position Size: {positionAmt}")
                if price < self.stopprofit or price >= self.stopprice:
                    self.order_Id = self.limit_buy(self.symbol,-positionAmt,price)
                    self.log("++++++++++++Closing Short++++++++++++")
                elif signal == 1:
                     self.log("++++++++++++Reverse Short to Long ++++++++++++")
                     self.order_Id = self.limit_sell(self.symbol,2*self.fixed_size,price)
                     self.stopprice = price+sigma
                     self.stopprofit = price - sigma*self.fixed_wlr
                else:
                    self.log(f"Entry Price: {entryPrice}")
                    self.log(f"Stop Loss at : {self.stopprice:.2f}")
                    self.log(f"Stop Profit at : {self.stopprofit:.2f}")  
        else:
            raise last_order["status"]  
        
def main_loop():
    # 初始化
    bot = kpattern_bot()
    #获取当前持仓信息
    positions = pd.DataFrame(bot.client.get_position_risk(recvWindow=6000))
    position = positions.query('symbol == @bot.symbol')
    positionAmt =  float(position['positionAmt'])
    entryPrice = float(position['entryPrice']) 
    #获取信号和波动空间sigma
    kdf_5m = bot.get_continuousklines(bot.symbol,"5m",100)
    bot.index_sig(kdf_5m)
    sigma = bot.index_signal["atr"][-1] * bot.atrk
    if positionAmt>0:
        bot.stopprice =  entryPrice - sigma
        bot.stopprofit = entryPrice + sigma
    if  positionAmt<0:
        bot.stopprice =  entryPrice + sigma
        bot.stopprofit = entryPrice - sigma

    bot.log("strategy initiating.....")

    def loop_1m():
        while True:
            try:
                # 管理仓位
                bot.manage_pos()
                # 睡眠等待下一轮循环
                time.sleep(5)
            except Exception as e:
                traceback.print_exc()
    def loop_5m():
        while True:
            try: 
                kdf_5m = bot.get_continuousklines(bot.symbol,"5m",100)
                #更新信号
                bot.index_sig(kdf_5m)
                # 睡眠等待下一轮循环
                time.sleep(150)
            except Exception as e:
                traceback.print_exc()
        
    t1 = threading.Thread(target=loop_1m)
    t2 = threading.Thread(target=loop_5m)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

main_loop()

