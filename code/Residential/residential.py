import pandas as pd
import os
import sys

sys.path.append('./code/')
from global_code import global_function as af
from Residential import residential_craw as rc

global_path = './data/'
raw_path = os.path.join(global_path, 'Residential', 'raw')
craw_path = os.path.join(global_path, 'Residential', 'craw')
useful_path = os.path.join(global_path, 'global_data')
out_path = os.path.join(global_path, 'Residential', 'cleaned')


def main():
    rc.main()  # 爬取并处理供热面积
    process()


def process():
    # 读取Residential的daily数据
    df_daily = af.read_daily(useful_path, 'Residential')
    df_daily['date'] = pd.to_datetime(df_daily['date'], format='%d/%m/%Y')
    df_daily['year'] = df_daily['date'].dt.year

    # 读取取暖面积的省级权重
    df_ratio = pd.read_csv(os.path.join(raw_path, 'ratio.csv'))
    # 行转列
    df_ratio = df_ratio.set_index(['Provinces']).stack().reset_index().rename(columns={'level_1': 'year', 0: 'ratio'})
    df_ratio['year'] = df_ratio['year'].astype(int)
    df_sum = df_ratio.groupby(['year']).sum().reset_index().rename(columns={'ratio': 'sum'})
    df_ratio = pd.merge(df_ratio, df_sum)
    df_ratio['ratio'] = df_ratio['ratio'] / df_ratio['sum']
    df_ratio = df_ratio[['Provinces', 'year', 'ratio']].rename(columns={'Provinces': 'state'})

    # 乘以ratio 但是少2022年
    df_result = pd.merge(df_daily, df_ratio)
    df_result['value'] = df_result['value'] * df_result['ratio']
    # 输出
    df_result = df_result[['date', 'state', 'value']]
    af.out_put(df_result, out_path, 'Residential')


if __name__ == '__main__':
    main()
