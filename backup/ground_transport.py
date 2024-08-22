import pandas as pd
import os
from pathlib import Path

import sys

env_path = '/data/xuanrenSong/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af
from Ground_Transport import ground_transport_craw as gc

global_path, raw_path, craw_path, useful_path, out_path = af.useful_element('Ground_Transport')


def main():
    gc.main()  # 爬取并整理汽车保有量数据
    process()


def process():
    # 读取Ground_Transport的daily数据
    df_daily = af.read_daily(useful_path, 'Ground Transport')
    df_daily['date'] = pd.to_datetime(df_daily['date'], format='%d/%m/%Y')
    df_daily['year'] = df_daily['date'].dt.year

    # 读取ratio
    df_ratio = pd.read_csv(Path(raw_path, 'ratio.csv'))
    # 行转列
    df_ratio = df_ratio.set_index(['Province']).stack().reset_index().rename(columns={'level_1': 'year', 0: 'ratio'})
    df_ratio['year'] = df_ratio['year'].astype(int)

    df_sum = df_ratio.groupby(['year']).sum(numeric_only=True).reset_index().rename(columns={'ratio': 'sum'})
    df_ratio = pd.merge(df_ratio, df_sum)
    df_ratio['ratio'] = df_ratio['ratio'] / df_ratio['sum']
    df_ratio = df_ratio[['Province', 'year', 'ratio']].rename(columns={'Province': 'state'})
    # 乘以ratio 但是少2022年
    df_result = pd.merge(df_daily, df_ratio)
    df_result['value'] = df_result['value'] * df_result['ratio']
    # 输出
    df_result = df_result[['date', 'state', 'value']]
    af.out_put(df_result, out_path, 'Ground_Transport')


if __name__ == '__main__':
    main()
