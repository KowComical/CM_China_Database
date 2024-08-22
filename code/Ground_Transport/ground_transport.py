import pandas as pd
import os
from pathlib import Path

import sys

env_path = '/data3/kow/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af
from Ground_Transport import ground_transport_craw as gc

global_path, raw_path, craw_path, useful_path, out_path = af.useful_element('Ground_Transport')


def main():
    gc.main()  # 爬取并整理汽车保有量数据
    process()


def process():
    # 读取Ground_Transport的daily数据
    df_daily = af.read_daily('Ground Transport')

    df_daily['year'] = df_daily['date'].dt.year

    # 计算ratio
    df_flow = pd.read_csv('/data3/kow/CM_Database/data/Ground_Transport/raw/china_拥堵延时指数.csv')

    df_flow['periodDate'] = pd.to_datetime(df_flow['periodDate'])

    df_flow.columns = ['date', 'value', 'city']

    # 填充缺失值
    df_flow = pd.pivot_table(df_flow,
                             index='date',
                             values='value',
                             columns='city').reset_index().sort_values(by=['date'])

    should_date = pd.date_range(start=min(df_flow['date']), end=max(df_flow['date'])).strftime('%Y-%m-%d')
    real_date = df_flow['date'].dt.strftime('%Y-%m-%d').unique()

    missing_date = list(set(should_date) - set(real_date))

    df_missing = pd.DataFrame(missing_date, columns=['date'])
    df_missing['date'] = pd.to_datetime(df_missing['date'])

    df_flow = pd.concat([df_flow, df_missing]).sort_values('date').reset_index(drop=True)
    # 取前后日平均值
    df_flow = df_flow.set_index('date').interpolate().reset_index()
    # 行转列
    df_flow = df_flow.melt(id_vars=['date'], var_name='city', value_name='value')  # 更有效率的办法

    df_p = pd.read_excel(os.path.join(raw_path, 'city_to_province.xlsx'))
    df_flow = pd.merge(df_flow, df_p)
    # 先按照平均值得出省级的Q值
    df_flow = df_flow.groupby(['state', 'date']).mean(numeric_only=True).reset_index()
    # 按照新疆算西藏 去掉香港
    df_flow = pd.pivot_table(df_flow, index='date', values='value', columns='state').reset_index()
    df_flow = df_flow.drop(columns=['香港'])
    df_flow['西藏'] = df_flow['新疆']
    df_flow = df_flow.melt(id_vars=['date'], var_name='state', value_name='value')  # 更有效率的办法
    # Regression Parameter Value
    a = 100.87
    b = 671.06
    c = 1.98
    d = 6.49
    x = df_flow['value']
    upper = b * (x ** c)
    bot = (d ** c) + (x ** c)
    result = a + upper / bot
    df_flow['flow'] = result
    df_flow = df_flow.drop(columns=['value'])
    # 再计算各省的每日占比
    df_ratio_q = pd.DataFrame()
    date_list = df_flow['date'].unique()
    for d in date_list:
        temp = df_flow[(df_flow['date'] == d)].reset_index(drop=True)
        temp_sum = temp.groupby(['date']).sum(numeric_only=True).reset_index().rename(columns={'flow': 'sum'})
        temp_ratio = pd.merge(temp, temp_sum)
        temp_ratio['ratio'] = temp_ratio['flow'] / temp_ratio['sum']
        temp_ratio = temp_ratio[['state', 'date', 'ratio']]
        df_ratio_q = pd.concat([df_ratio_q, temp_ratio]).reset_index(drop=True)
    # 将每日数据按照每日省份占比拆分
    df_result = pd.merge(df_daily, df_ratio_q, how='left')
    df_result['new_value'] = df_result['value'] * df_result['ratio']
    df_c = pd.read_csv(os.path.join(useful_path, 'city_name.csv'))
    df_result = pd.merge(df_result, df_c, left_on=['state'], right_on=['中文'], how='left')[
        ['date', 'new_value', '拼音']].rename(columns={'拼音': 'state', 'new_value': 'value'})
    # 输出
    df_result = df_result[['date', 'state', 'value']]

    af.out_put(df_result, out_path, 'Ground_Transport')


if __name__ == '__main__':
    main()
