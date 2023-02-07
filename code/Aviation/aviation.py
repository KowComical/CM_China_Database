import pandas as pd
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import sys

sys.path.append('./code/')
from global_code import global_function as af
import Aviation.aviation_craw as g

global_path = './data/'
raw_path = os.path.join(global_path, 'Aviation', 'raw')
craw_path = os.path.join(global_path, 'Aviation', 'craw')
useful_path = os.path.join(global_path, 'global_data')
out_path = os.path.join(global_path, 'Aviation', 'cleaned')


def main():
    g.main()  # 爬取pdf
    process()


def process():
    # 国内航空和国际航空的daily数据
    df_daily = af.read_daily(useful_path, 'Aviation')
    df_daily['date'] = pd.to_datetime(df_daily['date'], format='%d/%m/%Y')

    # 省级航空比例的数据
    df_ratio = pd.read_csv(os.path.join(raw_path, 'ratio.csv'))
    df_ratio['date'] = pd.to_datetime(df_ratio['date'])

    # # 乘以ratio
    df_result = pd.merge(df_daily, df_ratio)
    df_result['value'] = df_result['value'] * df_result['ratio']

    # 按照每日占比将月份拆分为日
    date_list = df_result['date'].unique()
    state_list = df_result['拼音'].unique()
    df_new = pd.DataFrame()
    for d in date_list:
        for s in state_list:
            temp = df_result[(df_result['date'] == d) & (df_result['拼音'] == s)].reset_index(drop=True)
            start_date = temp['date'][0].strftime('%Y-%m-%d')  # 起时日期
            end_date = (datetime.strptime(start_date, "%Y-%m-%d") + relativedelta(months=1) + timedelta(
                days=-1)).strftime('%Y-%m-%d')  # 结束日期
            # 当月范围天数
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            df_temp = pd.DataFrame(date_range, columns=['date'])
            df_temp['value'] = temp['value'][0] / len(date_range)  # 按天数平分
            df_temp['state'] = s
            df_new = pd.concat([df_new, df_temp]).reset_index(drop=True)

    # 输出
    df_new = df_new.rename(columns={'拼音': 'state'})[['date', 'state', 'value']]
    af.out_put(df_new, out_path, 'Aviation')


if __name__ == '__main__':
    main()
