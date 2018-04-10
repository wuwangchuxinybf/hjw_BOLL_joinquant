# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 12:57:43 2018

@author: bfyang.cephei
"""

# 克隆自聚宽文章：https://www.joinquant.com/post/8325
# 标题：量化“抓妖”新尝试——布林带“开口型喇叭口”股票投资策略
# 作者：ScintiGimcki

# 导入函数库
import jqdata
# 导入pandas
import pandas
# 导入技术分析指标函数
from jqlib.technical_analysis import *

# 初始化函数，设定基准等等
def initialize(context):
    # 设置参数
    set_params()
    # 设置回测条件
    set_backtest()
    
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG') 
      # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')

def set_params():
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 设定策略相关参数
    g.lag = 5    # 布林带收口时间窗口
    g.lim = 0.12   # 布林带带宽的极限
    g.Max = 1     # 最大持仓
    g.N = 0       # 持仓股数
    g.stock_set = '000300.XSHG' # 用来挑选股票的股票池
    
def set_backtest():
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')
    
## 开盘前运行函数     
def before_market_open(context):
    log.info(str('交易日期:'+str(context.current_dt)))
    # 如果有持仓股票，取出持仓股票代码，作为快收盘时监测是否卖出的股票池
    g.check_list = context.portfolio.positions.keys()
    g.N = len(g.check_list)
    # 如果持仓没有满，得到今日可以购买的股票列表
    log.info('g.N='+str(g.N))
    if g.N<g.Max:
        # 首先得到布林带宽在lim以内的股票
        g.buy_list = get_buy_list(context,g.lag,g.stock_set)
        if g.buy_list != []:
            # 得到今日股票前lag日的布林线数据
            up_line,mid_line,dn_line,width = get_bollinger(context,g.buy_list,g.lag)
            # 选取昨日放量上涨且收盘价位于布林中线以上且布林带放大的股票，
            # 依据昨日量价涨幅的综合评分对这些股票进行排序，取前Max-N个
            g.buy_list = grade_filter(g.buy_list,g.lag,up_line,width,context)
    # 如果持仓已满，则今天不购买股票
    else:
        g.buy_list = []
        log.info('今日不需购买股票')
    
    
def get_buy_list(context,lag,stk_set):
    # 先选出当日未停牌的股票
    # 得到当日是否停牌的dataframe，停牌为1，未停牌为0
    try:    # 若股票池为指数
        suspend_info = get_price(get_index_stocks(stk_set),start_date=context.current_dt,end_date=context.current_dt,frequency='daily',fields='paused')['paused'].T
    except: # 若股票池不为指数
        suspend_info = get_price(stk_set,start_date=context.current_dt,end_date=context.current_dt,frequency='daily',fields='paused')['paused'].T
    # 过滤掉停牌股票
    unsuspend_index = suspend_info.iloc[:,0]<1
    unsuspend_stock_ = list(suspend_info[unsuspend_index].index)
    # 进一步筛选出最近lag+1日未曾停牌的股票list
    unsuspend_stock = []
    for stock in unsuspend_stock_:
        if sum(attribute_history(stock,lag+1,'1d',('paused'),skip_paused=False))[0]==0:
            unsuspend_stock.append(stock)
    # 如果没有符合要求的股票则返回空
    if unsuspend_stock == []:
        log.info('没有过去十日没停牌的股票')
        return unsuspend_stock
    # 筛选出昨日前lag日布林带宽度在lim以内的股票
    up,mid,dn,wd = get_bollinger(context,unsuspend_stock,lag)
    narrow_index = wd.iloc[:,-2]<g.lim
    for day in range(2,lag):
        narrow_index = narrow_index&(wd.iloc[:,-day]<g.lim)
    narrow_stock = [unsuspend_stock[i] for i in [ind for ind,bool_value in enumerate(narrow_index) if bool_value==True]]
    if len(narrow_stock) != 0:
        log.info('今日潜在满足要求的标的有：'+str(len(narrow_stock)))
    return narrow_stock
    
def get_bollinger(context,buy,lag):
    # 创建以股票代码为index的dataframe对象来存储布林带信息
    dic = dict.fromkeys(buy,[0]*(lag+1)) # 创建一个以股票代码为keys的字典
    up = pandas.DataFrame.from_dict(dic).T # 用字典构造dataframe
    mid = pandas.DataFrame.from_dict(dic).T
    dn = pandas.DataFrame.from_dict(dic).T
    wd = pandas.DataFrame.from_dict(dic).T
    for stock in buy:
        for j in range(lag+1):
            up_,mid_,dn_ = Bollinger_Bands(stock,check_date=context.previous_date-datetime.timedelta(days=j),timeperiod=20,nbdevup=2,nbdevdn=2)
            up.loc[stock,j] = up_[stock]
            mid.loc[stock,j] = mid_[stock]
            dn.loc[stock,j] = dn_[stock]
            wd.loc[stock,j] = (up[j][stock] - dn[j][stock])/mid[j][stock]
    return up,mid,dn,wd
    
def grade_filter(buy,lag,up_line,wd,context):
    # 选出连续开口的股票
    open_index = wd.iloc[:,-1]<wd.iloc[:,-2]
    for day in range(1,lag-2):
        open_index = open_index&(wd.iloc[:,-day]<wd.iloc[:,-day-1])
    buy = [buy[i] for i in [ind for ind,bool_value in enumerate(open_index) if bool_value==True]]
    up_line = up_line[open_index]
    wd = wd[open_index]
    # 如果有连续开口的股票，则在连续开口的股票中进行下一步筛选
    if len(buy)>0:
        close_buy = history(lag+1,'1d','close',buy).T
        open_buy = history(lag+1,'1d','open',buy).T
        volume_buy = history(lag+1,'1d','volume',buy).T
        # 选取昨日放量上涨的股票，且收盘价位于中线上方，在上线的下方
        stock_rise_index = (close_buy.iloc[:,-1]>open_buy.iloc[:,-1])&(close_buy.iloc[:,-1]>close_buy.iloc[:,-2])&(volume_buy.iloc[:,-1]>volume_buy.iloc[:,-2])&(close_buy.iloc[:,-1]>up_line.iloc[:,-1])
        close_buy = close_buy[stock_rise_index]
        open_buy = open_buy[stock_rise_index]
        volume_buy = volume_buy[stock_rise_index]
        buy = list(close_buy.index)
        if len(buy)>0:
            # 用一个二维数组来存放股票的涨幅和量的涨幅
            portions = [([0]*2) for i in range(len(close_buy))]
            for i in range(len(close_buy)):
                portions[i][0] = (close_buy.iloc[i,0]-open_buy.iloc[i,0])/open_buy.iloc[i,0]
                portions[i][1] = (volume_buy.iloc[i,0]-volume_buy.iloc[i,1])/volume_buy.iloc[i,1]
            get_rank(portions)  # 将涨幅指标替换为排名指标
            grade = np.dot(portions,[[1.2],[0.5]])
            grade_rank(grade,buy)  # 对grade进行冒泡排序
            return buy[0:min(g.Max-g.N,len(buy))]
        else:
            return []
    else:
        return []
    
    
def get_rank(por):
    # 定义一个数组记录一开始的位置
    indexes = range(len(por))
    # 对每一列进行冒泡排序
    for col in range(len(por[0])):
        for row in range(len(por)):
            for nrow in range(row):
                if por[nrow][col]<por[row][col]:
                    indexes[nrow],indexes[row] = indexes[row],indexes[nrow]
                    for ecol in range(len(por[0])):
                        por[nrow][ecol],por[row][ecol] = por[row][ecol],por[nrow][ecol]
        for row in range(len(por)):
            por[row][col] = row
    # 再对indexes进行一次冒泡排序，使por恢复原顺序，每一行与buy中的股票代码相对应
    for row in range(len(por)):
        for nrow in range(row):
            if indexes[nrow]<indexes[row]:
                indexes[nrow],indexes[row] = indexes[row],indexes[nrow]
                for col in range(len(por[0])):
                    por[nrow][col],por[row][col] = por[row][col],por[nrow][col]
    return por
                    
def grade_rank(grades,buys):
    for row in range(len(grades)):
        for nrow in range(row):
            if grades[nrow]>grades[row]:
                grades[nrow],grades[row] = grades[row],grades[nrow]
                buys[nrow],buys[row] = buys[row],buys[nrow]
    return grades,buys
    

## 开盘时运行函数
def handle_data(context,data):
    # 每天开盘时
    if context.current_dt.hour==9 and context.current_dt.minute==30:
        if g.buy_list != []:
            for stock in g.buy_list:
                order_target_value(stock,context.portfolio.available_cash/(g.Max-g.N))
                g.N =g.N+1
    # 每天收盘时决定是否出售
    if context.current_dt.hour>=9 and context.current_dt.minute>30:
        if g.check_list != []:
            # 得到持仓股昨日的收盘价
            pre_close = history(1,'1d','close',g.check_list)
            # 得到持仓股昨日的布林线信息
            up_info,mid_info,dn_info,wth_info = get_bollinger(context,g.check_list,0)
            # 得到前一分钟价格
            pre_min_price = history(1,'1m','close',g.check_list)
            current_data = get_current_data()
            for stock in g.check_list:
                i = 0
                '''
                # 若当日股票下跌5%，卖出股票
                if pre_min_price[stock][-1]<0.95*pre_close[stock][-1]:
                    order_target_value(stock,0)
                '''
                if context.current_dt.hour==14 and context.current_dt.minute==53:
                    # 若收盘价格接近触碰中线，卖出股票
                    if pre_min_price[stock][-1]<=1.01*mid_info.iloc[0,-1]:
                        order_target_value(stock,0)
                    
    
 
## 收盘后运行函数  
def after_market_close(context):
    #得到当天所有成交记录
    trades = get_trades()
    for _trade in trades.values():
        log.info('成交记录：'+str(_trade))
    log.info('##############################################################')
