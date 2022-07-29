import pandas as pd
import os
import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af
from Industry import industry_craw as ic

global_path = './data/'
raw_path = os.path.join(global_path, 'Industry', 'raw')
craw_path = os.path.join(global_path, 'Industry', 'craw')
useful_path = os.path.join(global_path, 'global_data')
out_path = os.path.join(global_path, 'Industry', 'cleaned')


def main():
    ic.main()
    process()


def process():
    df_sector = pd.read_csv(os.path.join(raw_path, 'ratio.csv'))  # 各部门的Ratio数据
    sector_list = df_sector['部门'].drop_duplicates().tolist()  # 各部门
    ratio_list = df_sector['Ratio'].drop_duplicates().tolist()  # 各部门ratio
    # 读取industry的daily数据
    df_daily = af.read_daily(useful_path, 'Industry')
    for x, y in zip(sector_list, ratio_list):
        df_daily[x] = df_daily['value'] * y
    df_daily = df_daily.drop(columns=['value'])
    df_daily = df_daily.set_index(['date']).stack().reset_index().rename(columns={'level_1': '部门', 0: '值'})
    df_daily['date'] = pd.to_datetime(df_daily['date'], format='%d/%m/%Y')
    df_daily['year'] = df_daily['date'].dt.year
    df_daily['month'] = df_daily['date'].dt.month

    # 根据需求 读取所有月排放数据
    df_history = pd.read_csv(os.path.join(craw_path, 'Industry_raw.csv'))
    xuqiu = df_sector['类型'].tolist()  # 读取需求的部门
    df_data = pd.DataFrame()
    for xu in xuqiu:
        xu = xu.replace('(', '[(]').replace(')', '[)]')  # 带括号的找不到 所以修改一下
        temp = df_history[df_history['type'].str.contains(xu)].reset_index(drop=True)
        if not temp.empty:
            df_data = pd.concat([df_data, temp]).reset_index(drop=True)
        else:
            print(xu)

    # # 数据清洗
    df_data = df_data.groupby(['name', 'date', 'type']).mean().reset_index()
    df_data = df_data.rename(columns={'type': '指标', 'date': '时间', 'data': '值', 'name': '省市名称'})

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
        df_dangqi = df_dangqi.set_index(['地区', '类型']).stack().reset_index().rename(
            columns={'level_2': 'date', 0: 'value'})
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
    df_result['daily'] = df_result['daily']
    df_result = df_result.rename(columns={'拼音': 'state', 'daily': 'value'})

    # 输出
    df_result = df_result[['date', 'state', 'value']]
    af.out_put(df_result, out_path, 'Industry')


if __name__ == '__main__':
    main()
