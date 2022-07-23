import pandas as pd
import os
import sys

sys.dont_write_bytecode = True
sys.path.append('K:\\Github\\CM_China_Database\\code\\')
from global_code import global_function as af

global_path = 'K:\\Github\\CM_China_Database\\data\\'
raw_path = os.path.join(global_path, 'Residential', 'raw')
craw_path = os.path.join(global_path, 'Residential', 'craw')
useful_path = os.path.join(global_path, 'global_data')
out_path = os.path.join(global_path, 'Residential', 'cleaned')

# 读取Residential的daily数据
df_daily = af.read_daily(useful_path, 'Residential')
df_daily['date'] = pd.to_datetime(df_daily['date'])
df_daily['year'] = df_daily['date'].dt.year

# 读取取暖面积的省级权重
df_ratio = pd.read_csv(os.path.join(raw_path, 'Heating area from 2014 to 2020.csv'))
df_ratio = df_ratio[df_ratio['Provinces'] != 'Total'].reset_index(drop=True)
# 行转列
df_ratio = df_ratio.set_index(['Provinces']).stack().reset_index().rename(columns={'level_1': 'year', 0: 'ratio'})
df_ratio['year'] = df_ratio['year'].astype(int)
df_sum = df_ratio.groupby(['year']).sum().reset_index().rename(columns={'ratio': 'sum'})
df_ratio = pd.merge(df_ratio, df_sum)
df_ratio['ratio'] = df_ratio['ratio'] / df_ratio['sum']
df_ratio = df_ratio[['Provinces', 'year', 'ratio']].rename(columns={'Provinces': 'state'})

# 乘以ratio 但是少2022年
df_result = pd.merge(df_daily, df_ratio)
df_result['value'] = df_result['co2'] * df_result['ratio'] / 1000
# 输出
df_result = df_result[['date', 'state', 'value']]
af.out_put(df_result, out_path, 'Residential')
