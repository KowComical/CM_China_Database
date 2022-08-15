import pandas as pd
import os
import aviation_craw as g
import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af

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

    # 输出
    df_result = df_result[['date', 'state', 'value']].rename(columns={'拼音': 'state'})
    af.out_put(df_result, out_path, 'Aviation')


if __name__ == '__main__':
    main()
