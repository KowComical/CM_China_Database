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
    df_daily['year'] = df_daily['date'].dt.year

    # 省级航空比例的数据
    df_ratio = pd.read_csv(os.path.join(raw_path, 'ratio.csv'))
    df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv'))
    df_ratio = pd.merge(df_ratio, df_city, left_on='Province', right_on='中文')[['拼音', '2019', '2020', '2021']]  # 城市改英文
    df_ratio = df_ratio.set_index(['拼音']).stack().reset_index().rename(
        columns={'level_1': 'year', 0: 'ratio', '拼音': 'state'})
    df_ratio['year'] = df_ratio['year'].astype(int)

    # 乘以ratio
    df_result = pd.merge(df_daily, df_ratio)
    df_result['value'] = df_result['value'] * df_result['ratio'] / 100

    # 输出
    df_result = df_result[['date', 'state', 'value']]
    af.out_put(df_result, out_path, 'Aviation')


if __name__ == '__main__':
    main()
