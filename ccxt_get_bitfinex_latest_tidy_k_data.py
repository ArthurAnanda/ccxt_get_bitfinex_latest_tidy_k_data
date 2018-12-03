import ccxt
import datetime
import time
import pandas as pd

'''
author: Arthur Luo
3.6.4 |Anaconda custom (64-bit)
'''

pd.set_option('expand_frame_repr', False)
pd.set_option('max_rows', 20)


def run_function_till_success(function, tryTimes=5):
    '''
    将函数function尝试运行tryTimes次，直到成功返回函数结果和运行次数，否则返回False
    '''
    retry = 0
    while True:
        if retry > tryTimes:
            return False
        try:
            result = function()
            return [result, retry]
        except:
            retry += 1


def ccxt_get_bitfinex_k_data(symbol='BTC/USDT', timeframe='1m', start_time_str='2018-12-03 00:00:00', limit=1000, timeout=1000):
    '''
    从一个开始时点获取之后的limit根k线数据
    :param symbol: str, just like 'BTC/USDT'
    :param timeframe: str, 三个取值（'1m'， '5m'， '15m'）
    :param start_time_str: str,开始时间点
    :param limit: int
    :param timeout: int，超时毫秒（超时秒数乘以1000）
    :return: pandas.DataFrame
    '''
    bitfinex = ccxt.bitfinex({'timeout': timeout})
    local_timestamp = pd.to_datetime(start_time_str).timestamp()
    utc_timestamp = local_timestamp - 8 * 60 * 60
    data = bitfinex.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit, since=utc_timestamp * 1000)
    result = pd.DataFrame(data)
    result.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
    result['time'] = pd.to_datetime(result['time'], unit='ms') + pd.Timedelta(hours=8)
    return result


def ccxt_get_bitfinex_latest_k_data(symbol='BTC/USDT', timeframe='5m', limit=1000, timeout=1000):
    '''
    使用ccxt接口获取bitfinex交易所最近的limit根k线数据（当前时点未走完的k线被剔除）
    注意：如果某根K线的交易量为空，那么会缺失这一根k线，因为bitfinex的接口不会提供交易量为零的k线数据，这种情况在1分钟k线数据上很常见，所以1分钟k线个数少于limit也是正常的。
    :param symbol: str, just like 'BTC/USDT'
    :param timeframe: str, 三个取值（'1m'， '5m'， '15m'）
    :param limit: int
    :param timeout: int，超时毫秒（超时秒数乘以1000）
    :return: pandas.DataFrame
    '''
    now_datetime = datetime.datetime.now()
    time_frame = int(timeframe.replace('m', ''))
    end_minute = (now_datetime.minute // time_frame) * time_frame - time_frame
    timetemp = datetime.datetime(year=now_datetime.year, month=now_datetime.month, day=now_datetime.day, hour=now_datetime.hour, minute=end_minute)
    timeendstr = timetemp.strftime('%Y-%m-%d %H:%M:%S')
    timestart = timetemp - datetime.timedelta(minutes=time_frame * (limit - 1))
    timestart = timestart.strftime('%Y-%m-%d %H:%M:%S')
    result = ccxt_get_bitfinex_k_data(symbol=symbol, timeframe=timeframe, start_time_str=timestart, limit=limit, timeout=timeout)
    if str(result.iloc[-1]['time']) > timeendstr:
        result = result.iloc[:-1, :]
    else:
        pass
    return result


tryTimes = 5
timeframe = '15m'
print(timeframe + ' K线数据：')
print('当前时间点：', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print('-' * 100)
result = run_function_till_success(function=lambda: ccxt_get_bitfinex_latest_k_data(timeframe=timeframe), tryTimes=tryTimes)
if result:
    print(result[0])
    if result[1]:
        print('-' * 100)
        print(f'重复获取次数：{result[1]}')
else:
    print(f'{tryTimes}次尝试获取失败，请检查网络以及参数')
