import pandas as pd
import os
from datetime import datetime

import sys

sys.dont_write_bytecode = True
sys.path.append('K:\\Github\\中国CM网站\\code\\')
from global_code import global_function as af

global_path = 'K:\\Github\\中国CM网站\\data\\'
raw_path = os.path.join(global_path, 'Power', 'raw')
craw_path = os.path.join(global_path, 'Power', 'craw')
useful_path = os.path.join(global_path, 'global_data')
out_path = os.path.join(global_path, 'Power', 'cleaned')

# 读取daily数据 并计算占比
cm_path = af.search_file(useful_path)
cm_path = [cm_path[i] for i, x in enumerate(cm_path) if x.find('CM_v') != -1][0]
df_daily = pd.read_csv(cm_path)  # 全国日排放
df_daily = df_daily[(df_daily['country'] == 'China') & (df_daily['sector'] == 'Power')].reset_index(drop=True)
df_daily['date'] = pd.to_datetime(df_daily['date'])
df_daily['year'] = df_daily['date'].dt.year
df_daily['month'] = df_daily['date'].dt.month
df_sum = df_daily.groupby(['year', 'month']).sum().reset_index().rename(columns={'co2': 'sum'})
df_ratio = pd.merge(df_daily, df_sum)
df_ratio['ratio'] = df_ratio['co2'] / df_ratio['sum']
df_ratio = df_ratio[['year', 'month', 'ratio', 'date']]

# 读取当期和累计数据
file_name = af.search_file(craw_path)
dangqi_path = [file_name[i] for i, x in enumerate(file_name) if x.find('当期') != -1][0]
leiji_path = [file_name[i] for i, x in enumerate(file_name) if x.find('累计') != -1][0]
df_dangqi = pd.read_excel(dangqi_path, sheet_name='分省月度数据', header=3).dropna(axis=0, how='all', thresh=2).reset_index(
    drop=True)  # 非空值小于2时删除行
df_leiji = pd.read_excel(leiji_path, sheet_name='分省月度数据', header=3).dropna(axis=0, how='all', thresh=2).reset_index(
    drop=True)  # 非空值小于2时删除行

# 读取工作日
work = pd.read_csv(os.path.join(useful_path, 'workday.csv'))
work['date'] = work['year'].astype(str) + '年' + work['month'].astype(str) + '月'

# 补全缺失的1&2月当月值
min_year = int(min(df_dangqi.columns[1:])[:4])
max_year = int(max(df_dangqi.columns[1:])[:4])
for i in range(min_year, max_year + 1):  # 按照当前的年份的最小值和最大值来填充1月和2月数据
    df_dangqi['%s年1月' % i] = df_leiji['%s年2月' % i] * work[work['date'] == '%s年1月' % i]['ratio'].values
    df_dangqi['%s年2月' % i] = df_leiji['%s年2月' % i] * work[work['date'] == '%s年2月' % i]['ratio'].values

# 整理当期数据
df_dangqi = df_dangqi.set_index(['地区']).stack().reset_index().rename(columns={'level_1': 'date', 0: 'value'})
df_dangqi['date'] = pd.to_datetime(df_dangqi['date'], format='%Y年%m月')
df_dangqi['year'] = df_dangqi['date'].dt.year
df_dangqi = df_dangqi.rename(columns={'地区': 'state'})
df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv')).rename(columns={'全称': 'state'}).drop(columns=['地区'])
df_dangqi = pd.merge(df_dangqi, df_city)[['date', 'year', '拼音', 'value']].rename(columns={'拼音': 'state'})

# 乘以排放因子 # 并不知道这个排放因子是如何得来的 而且为什么推算的时候要乘以8 这里暂用现成的 未来要更新这里
df_ef = pd.read_csv(os.path.join(raw_path, 'ef.csv'))
df_ef = df_ef.set_index(['省级电网']).stack().reset_index().rename(columns={'level_1': 'time', 0: 'ef'}).rename(
    columns={'省级电网': 'state'})
df_ef['year'] = ('20' + df_ef['time'].str.replace('年排放因子', '').str.replace(' ', '')).astype(int)
df_ef = pd.merge(df_ef, df_city, left_on='state', right_on='中文')[['year', '拼音', 'ef']].rename(columns={'拼音': 'state'})

df_result = pd.merge(df_dangqi, df_ef)
df_result['value'] = df_result['value'] * df_result['ef'] * 0.1  # 将所有值都乘以0.1 保持单位一致
df_result['month'] = df_result['date'].dt.month
df_result = df_result.drop(columns=['ef', 'date'])

# # 将结果拆分到daily
df_result = pd.merge(df_result, df_ratio)
df_result['value'] = df_result['value'] * df_result['ratio']
df_result = df_result[['date', 'state', 'value']]
df_result['sector'] = 'Power'
# 输出输出两个版本
now_date = datetime.now().strftime('%Y-%m-%d')
# 第一个带日期放在history文件夹里备用
df_result.to_csv(os.path.join(out_path, 'history', 'power_result_%s.csv' % now_date), index=False,
                 encoding='utf_8_sig')
# 第二个放在外面当最新的用
df_result.to_csv(os.path.join(out_path, 'power_result.csv'), index=False, encoding='utf_8_sig')
