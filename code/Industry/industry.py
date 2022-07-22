import pandas as pd
import os
from datetime import datetime

import sys

sys.dont_write_bytecode = True
sys.path.append('K:\\Github\\中国CM网站\\code\\')
from global_code import global_function as af

global_path = 'K:\\Github\\中国CM网站\\data\\'
raw_path = os.path.join(global_path, 'Industry', 'raw')
craw_path = os.path.join(global_path, 'Industry', 'craw')
useful_path = os.path.join(global_path, 'global_data')
out_path = os.path.join(global_path, 'Industry', 'cleaned')

df_sector = pd.read_csv(os.path.join(raw_path, 'ratio.csv'))  # 各部门的Ratio数据
sector_list = df_sector['部门'].drop_duplicates().tolist()  # 各部门
ratio_list = df_sector['Ratio'].drop_duplicates().tolist()  # 各部门ratio
cm_path = af.search_file(useful_path)
cm_path = [cm_path[i] for i, x in enumerate(cm_path) if x.find('CM_v') != -1][0]
df_daily = pd.read_csv(cm_path)  # 全国日排放
df_daily = df_daily[(df_daily['country'] == 'China') & (df_daily['sector'] == 'Industry')].reset_index(drop=True)
for x, y in zip(sector_list, ratio_list):
    df_daily[x] = df_daily['co2'] * y
df_daily = df_daily.drop(columns=['country', 'co2', 'sector'])
df_daily = df_daily.set_index(['date']).stack().reset_index().rename(columns={'level_1': '部门', 0: '值'})
df_daily['date'] = pd.to_datetime(df_daily['date'])
df_daily['year'] = df_daily['date'].dt.year
df_daily['month'] = df_daily['date'].dt.month

# 根据需求 读取所有月排放数据
file_name = af.search_file(craw_path)
xuqiu = df_sector['类型'].tolist()  # 读取需求的部门
# 筛选
df_data = pd.DataFrame()
for y in xuqiu:
    result = [file_name[i] for i, x in enumerate(file_name) if x.find(y) != -1][0]
    temp = pd.read_csv(result)
    df_data = pd.concat([df_data, temp]).reset_index(drop=True)
# 数据清洗
# 处理缺失省份的问题
df_data = pd.pivot_table(df_data, index=['指标', '时间'], values='值', columns='省市名称').fillna(0).reset_index()
df_data = df_data.set_index(['指标', '时间']).stack().reset_index().rename(columns={0: '值'})

df_data = df_data[['指标', '省市名称', '时间', '值']].reset_index(drop=True)
df_data = df_data[df_data['时间'] >= 201901].reset_index(drop=True)  # 只要2019年之后的数据

# 将日期格式统一为中文
df_data['时间'] = pd.to_datetime(df_data['时间'], format='%Y%m').astype(str)
df_data['时间'] = df_data['时间'].str.replace('-01', '月').str.replace('-', '年').str.replace('年0', '年')

# 读取工作日
work = pd.read_csv(os.path.join(useful_path, 'workday.csv'))
work['date'] = work['year'].astype(str) + '年' + work['month'].astype(str) + '月'

# 补全缺失的1&2月当月值
all_type = df_data['指标'].drop_duplicates().tolist()  # 将所有的类型都提起出来放在列表里

dangqi_list = [all_type[i] for i, x in enumerate(all_type) if x.find('当') != -1]
leiji_list = [all_type[i] for i, x in enumerate(all_type) if x.find('累计') != -1]  # 按照当期累计分类成两个列表

df_result = pd.DataFrame()
for D, L in zip(dangqi_list, leiji_list):
    df_dangqi = df_data[df_data['指标'] == D]
    df_dangqi = pd.pivot_table(df_dangqi, index='省市名称', values='值', columns='时间').reset_index()
    df_dangqi = df_dangqi.rename(columns={'省市名称': '地区'})

    df_leiji = df_data[df_data['指标'] == L]
    df_leiji = pd.pivot_table(df_leiji, index='省市名称', values='值', columns='时间').reset_index()
    df_leiji = df_leiji.rename(columns={'省市名称': '地区'})

    min_year = int(min(df_dangqi.columns[1:])[:4])
    max_year = int(max(df_dangqi.columns[1:])[:4])
    for i in range(min_year, max_year + 1):  # 按照当前的年份的最小值和最大值来填充1月和2月数据
        df_dangqi['%s年1月' % i] = df_leiji['%s年2月' % i] * work[work['date'] == '%s年1月' % i]['ratio'].values
        df_dangqi['%s年2月' % i] = df_leiji['%s年2月' % i] * work[work['date'] == '%s年2月' % i]['ratio'].values
    df_dangqi['类型'] = D.replace('产量_当月值', '')
    df_dangqi = df_dangqi.set_index(['地区', '类型']).stack().reset_index().rename(columns={'level_2': 'date', 0: 'value'})
    df_result = pd.concat([df_result, df_dangqi]).reset_index(drop=True)  # 合并所有结果到新的df
# 计算占比
df_result = pd.merge(df_sector, df_result).drop(columns=['Ratio', '数据名称'])
df_sum = df_result.groupby(['时间', '类型']).sum().reset_index().rename(columns={'value': 'sum'})
df_result = pd.merge(df_result, df_sum)
df_result['ratio'] = df_result['value'] / df_result['sum']
df_result = df_result.drop(columns=['value', 'sum'])
df_ratio = df_result.groupby(['部门', '地区', '时间']).mean().reset_index()
# 按照占比拆分daily数据
df_ratio['时间'] = pd.to_datetime(df_ratio['时间'], format='%Y年%m月')
df_ratio['year'] = df_ratio['时间'].dt.year
df_ratio['month'] = df_ratio['时间'].dt.month
df_daily['year'] = df_daily['date'].dt.year
df_daily['month'] = df_daily['date'].dt.month

df_result = pd.merge(df_daily, df_ratio)
df_result['daily'] = df_result['值'] * df_result['ratio']
df_result = df_result[['date', '部门', '地区', 'daily']]
# 中文改英文名
df_result = df_result.groupby(['date', '地区']).sum().reset_index().rename(columns={'地区': 'city'})
df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv')).rename(columns={'全称': 'city'})
df_result = pd.merge(df_result, df_city)[['date', '拼音', 'daily']]
df_result['daily'] = df_result['daily'] / 1000  # 换单位
df_result = df_result.rename(columns={'拼音': 'state', 'daily': 'value'})
df_result['sector'] = 'Industry'
# 输出输出两个版本
now_date = datetime.now().strftime('%Y-%m-%d')
# 第一个带日期放在history文件夹里备用
df_result.to_csv(os.path.join(out_path, 'history', 'industry_result_%s.csv' % now_date), index=False,
                 encoding='utf_8_sig')
# 第二个放在外面当最新的用
df_result.to_csv(os.path.join(out_path, 'industry_result.csv'), index=False, encoding='utf_8_sig')
