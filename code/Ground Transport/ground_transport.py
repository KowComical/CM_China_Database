import pandas as pd
import os
import sys

sys.dont_write_bytecode = True
sys.path.append('K:\\Github\\CM_China_Database\\code\\')
from global_code import global_function as af

global_path = 'K:\\Github\\CM_China_Database\\data\\'
raw_path = os.path.join(global_path, 'Ground Transport', 'raw')
craw_path = os.path.join(global_path, 'Ground Transport', 'craw')
useful_path = os.path.join(global_path, 'Global Data')
out_path = os.path.join(global_path, 'Ground Transport', 'cleaned')

# 读取Ground Transport的daily数据
df_daily = af.read_daily(useful_path, 'Ground Transport')
df_daily['date'] = pd.to_datetime(df_daily['date'], format='%d/%m/%Y')
df_daily['year'] = df_daily['date'].dt.year

# 读取ratio
df_ratio = pd.read_csv(os.path.join(raw_path, 'Provincial yearly Car Parc_2015_2021.csv'))
# 行转列
df_ratio = df_ratio.set_index(['Province']).stack().reset_index().rename(columns={'level_1': 'year', 0: 'ratio'})
df_ratio['year'] = df_ratio['year'].astype(int)
df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv'))
df_ratio = pd.merge(df_ratio, df_city, left_on='Province', right_on='全称')[['拼音', 'year', 'ratio']]  # 城市改英文
df_sum = df_ratio.groupby(['year']).sum().reset_index().rename(columns={'ratio': 'sum'})
df_ratio = pd.merge(df_ratio, df_sum)
df_ratio['ratio'] = df_ratio['ratio'] / df_ratio['sum']
df_ratio = df_ratio[['拼音', 'year', 'ratio']].rename(columns={'拼音': 'state'})
# 乘以ratio 但是少2022年
df_result = pd.merge(df_daily, df_ratio)
df_result['value'] = df_result['value'] * df_result['ratio']
# 输出
df_result = df_result[['date', 'state', 'value']]
af.out_put(df_result, out_path, 'Ground Transport')
