import pandas as pd
import os
import sys
from datetime import datetime

env_path = '/data3/kow/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af
import Industry.industry_craw as ic

global_path, raw_path, craw_path, useful_path, out_path = af.useful_element('Industry')


def main():
    # 获取当前日期和时间
    now = datetime.now()
    # 检查今天是否是星期二（.weekday() 返回0代表星期一，1代表星期二，等等）
    if now.weekday() == 1:  # 1 代表星期二
        ic.main()

    process_all()


def process_old():
    df_sector = pd.read_csv(os.path.join(raw_path, 'ratio.csv'))  # 各部门的Ratio数据

    sector_list = df_sector['部门'].unique()  # 各部门
    ratio_list = df_sector['Ratio'].unique()  # 各部门ratio
    # 读取industry的daily数据
    df_daily = af.read_daily('Industry')
    for x, y in zip(sector_list, ratio_list):
        df_daily[x] = df_daily['value'] * y
    df_daily = df_daily.drop(columns=['value'])
    df_daily = df_daily.set_index(['date']).stack().reset_index().rename(columns={'level_1': '部门', 0: '值'})

    df_daily['year'] = df_daily['date'].dt.year
    df_daily['month'] = df_daily['date'].dt.month

    # 根据需求 读取所有月排放数据
    df_history = pd.read_csv(os.path.join(craw_path, 'industry_raw.csv'))
    xuqiu = df_sector['类型'].tolist()  # 读取需求的部门
    df_data = pd.DataFrame()
    for xu in xuqiu:
        xu = xu.replace('(', '[(]').replace(')', '[)]')  # 带括号的找不到 所以修改一下
        temp = df_history[df_history['type'].str.contains(xu)].reset_index(drop=True)
        if not temp.empty:
            df_data = pd.concat([df_data, temp]).reset_index(drop=True)
        else:
            print(xu)

    # 数据清洗
    df_data = df_data.groupby(['name', 'date', 'type']).mean(numeric_only=True).reset_index()
    df_data = df_data.rename(columns={'type': '指标', 'date': '时间', 'data': '值', 'name': '省市名称'})
    # 读取工作日
    work = af.process_workday()
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

        # 去除还没有2月的1月
        df_dangqi = af.find_missing_month(df_dangqi)
        df_leiji = af.find_missing_month(df_leiji)

        min_year = int(min(df_dangqi.columns[1:])[:4])
        max_year = int(max(df_dangqi.columns[1:])[:4])
        for year in range(min_year, max_year + 1):  # 按照当前的年份的最小值和最大值来填充1月和2月数据
            # 定义1月和2月的变量
            jan_month = f'{year}年1月'
            feb_month = f'{year}年2月'

            df_dangqi[jan_month] = df_leiji[feb_month] * work[work['date'] == jan_month]['ratio'].values
            df_dangqi[feb_month] = df_leiji[feb_month] * work[work['date'] == feb_month]['ratio'].values

        df_dangqi['类型'] = D.replace('产量_当月值', '')
        df_dangqi = df_dangqi.set_index(['地区', '类型']).stack().reset_index().rename(
            columns={'level_2': 'date', 0: 'value'})
        df_result = pd.concat([df_result, df_dangqi]).reset_index(drop=True)  # 合并所有结果到新的df
    # # 计算占比
    df_result = pd.merge(df_sector, df_result).drop(columns=['Ratio', '数据名称'])
    df_sum = df_result.groupby(['时间', '类型']).sum(numeric_only=True).reset_index().rename(columns={'value': 'sum'})
    df_result = pd.merge(df_result, df_sum)
    df_result['ratio'] = df_result['value'] / df_result['sum']
    df_result = df_result.drop(columns=['value', 'sum'])
    df_ratio = df_result.groupby(['部门', '地区', '时间']).mean(numeric_only=True).reset_index()
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
    df_result = df_result.groupby(['date', '地区']).sum(numeric_only=True).reset_index().rename(
        columns={'地区': 'city'})
    df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv')).rename(columns={'全称': 'city'})
    df_result = pd.merge(df_result, df_city)[['date', '拼音', 'daily']]
    df_result['daily'] = df_result['daily']
    df_result = df_result.rename(columns={'拼音': 'state', 'daily': 'value'})
    df_result['state'] = df_result['state'].replace('InnerMongolia', 'Inner Mongolia')
    # 输出
    df_result = df_result[['date', 'state', 'value']]
    df_result['类型'] = 'Total'

    return df_result


def fill_missing(type_name):
    df = pd.read_csv(os.path.join(craw_path, 'industry_raw.csv'))
    df = df[df['type'].str.contains(f'{type_name}产量')].reset_index(drop=True)
    df = df.rename(columns={'type': '指标', 'date': '时间', 'data': '值', 'name': '省市名称'})
    # 补全缺失的1&2月当月值
    work = af.process_workday()

    all_type = df['指标'].drop_duplicates().tolist()  # 将所有的类型都提起出来放在列表里
    dangqi_list = [all_type[i] for i, x in enumerate(all_type) if x.find('当') != -1]
    leiji_list = [all_type[i] for i, x in enumerate(all_type) if x.find('累计') != -1]  # 按照当期累计分类成两个列表

    df_data = df.copy()
    df_result = pd.DataFrame()
    for D, L in zip(dangqi_list, leiji_list):
        df_dangqi = df_data[df_data['指标'] == D]
        df_dangqi = pd.pivot_table(df_dangqi, index='省市名称', values='值', columns='时间').reset_index()
        df_dangqi = df_dangqi.rename(columns={'省市名称': '地区'})

        df_leiji = df_data[df_data['指标'] == L]
        df_leiji = pd.pivot_table(df_leiji, index='省市名称', values='值', columns='时间').reset_index()
        df_leiji = df_leiji.rename(columns={'省市名称': '地区'})

        # 去除还没有2月的1月
        df_dangqi = af.find_missing_month(df_dangqi)
        df_leiji = af.find_missing_month(df_leiji)

        min_year = int(min(df_dangqi.columns[1:])[:4])
        max_year = int(max(df_dangqi.columns[1:])[:4])
        for year in range(min_year, max_year + 1):  # 按照当前的年份的最小值和最大值来填充1月和2月数据
            # 定义1月和2月的变量
            jan_month = f'{year}年1月'
            feb_month = f'{year}年2月'

            df_dangqi[jan_month] = df_leiji[feb_month] * work[work['date'] == jan_month]['ratio'].values
            df_dangqi[feb_month] = df_leiji[feb_month] * work[work['date'] == feb_month]['ratio'].values

        df_dangqi['类型'] = D.replace('产量_当月值', '')
        df_dangqi = df_dangqi.set_index(['地区', '类型']).stack().reset_index().rename(
            columns={'level_2': 'date', 0: 'value'})
        df_result = pd.concat([df_result, df_dangqi]).reset_index(drop=True)  # 合并所有结果到新的df
    # 稍作整理
    df_result['date'] = pd.to_datetime(df_result['时间'], format='%Y年%m月')
    df_result['year'] = df_result['date'].dt.year

    return df_result


def process_quanguo(type_dangqi_code, type_leiji_code):
    end_year = datetime.now().strftime('%Y%m%d')
    df_jing_dangqi = af.get_json('hgyd', type_dangqi_code, '2005-%s' % end_year, jing=True)
    df_jing_leiji = af.get_json('hgyd', type_leiji_code, '2005-%s' % end_year, jing=True)
    df_jing = pd.concat([df_jing_dangqi, df_jing_leiji]).reset_index(drop=True)
    df_jing = df_jing.rename(columns={'name': '指标', 'date': '时间', 'data': '值'})
    # 补全缺失的1&2月当月值
    work = af.process_workday()

    all_type = df_jing['指标'].drop_duplicates().tolist()  # 将所有的类型都提起出来放在列表里
    dangqi_list = [all_type[i] for i, x in enumerate(all_type) if x.find('当') != -1]
    leiji_list = [all_type[i] for i, x in enumerate(all_type) if x.find('累计') != -1]  # 按照当期累计分类成两个列表

    df_data = df_jing.copy()
    df_result = pd.DataFrame()
    for D, L in zip(dangqi_list, leiji_list):
        df_dangqi = df_data[df_data['指标'] == D]
        df_dangqi = pd.pivot_table(df_dangqi, index='指标', values='值', columns='时间').reset_index()

        df_leiji = df_data[df_data['指标'] == L]
        df_leiji = pd.pivot_table(df_leiji, index='指标', values='值', columns='时间').reset_index()

        # 去除还没有2月的1月
        df_dangqi = af.find_missing_month(df_dangqi)
        df_leiji = af.find_missing_month(df_leiji)

        min_year = int(min(df_dangqi.columns[1:])[:4])
        max_year = int(max(df_dangqi.columns[1:])[:4])
        for year in range(min_year, max_year + 1):  # 按照当前的年份的最小值和最大值来填充1月和2月数据
            # 定义1月和2月的变量
            jan_month = f'{year}年1月'
            feb_month = f'{year}年2月'

            df_dangqi[jan_month] = df_leiji[feb_month] * work[work['date'] == jan_month]['ratio'].values
            df_dangqi[feb_month] = df_leiji[feb_month] * work[work['date'] == feb_month]['ratio'].values

        df_dangqi['类型'] = D.replace('产量_当期值', '')
        df_dangqi = df_dangqi.set_index(['指标', '类型']).stack().reset_index().rename(
            columns={'level_2': 'date', 0: 'value'})
        df_result = pd.concat([df_result, df_dangqi]).reset_index(drop=True)  # 合并所有结果到新的df
    # 稍作整理

    df_result['date'] = pd.to_datetime(df_result['时间'], format='%Y年%m月')
    df_result['year'] = df_result['date'].dt.year
    df_result = df_result.drop(columns=['指标', '类型'])

    return df_result


# 钢铁
def process_gangcai(type_name):
    df_gangcai = fill_missing(type_name)
    # 19年之后计算
    province_ratio = pd.read_excel(os.path.join(craw_path, 'industry_ratio.xlsx'), sheet_name=type_name)
    df_province = pd.merge(df_gangcai, province_ratio)
    df_province['chang_value'] = df_province['value'] * df_province['长流程高炉'] / 100 * 2.23
    df_province['duan_value'] = df_province['value'] * df_province['短流程电炉'] / 100 * 0.85
    df_province['value'] = df_province[['chang_value', 'duan_value']].sum(axis=1)
    df_province = df_province[['地区', '类型', 'date', 'value']]
    return df_province


# 水泥
def process_cement(type_name):
    df_cement = fill_missing(type_name)
    province_ratio = pd.read_excel(os.path.join(craw_path, 'industry_ratio.xlsx'), sheet_name=type_name)
    df_province = pd.merge(df_cement, province_ratio)
    df_province['value'] = df_province['value'] * df_province['各省熟料比'] * 0.538
    df_province = df_province[['地区', '类型', 'date', 'value']]
    return df_province


# 铜冶炼
def process_cube(type_name):
    # 先计算比例
    df_cube = fill_missing(type_name).rename(columns={'value': type_name})
    df_old_cube = df_cube.drop(columns=['year']).groupby(['date']).sum(numeric_only=True).reset_index()
    df_jing_cube = process_quanguo('A02091E01', 'A02091E02').rename(columns={'value': '精炼铜'})
    df_ratio = pd.merge(df_old_cube, df_jing_cube)
    df_ratio['ratio'] = df_ratio['精炼铜'] / df_ratio[type_name]
    df_ratio = df_ratio[['date', 'ratio']]
    df_cube = pd.merge(df_cube, df_ratio)
    # 计算
    df_cube['value'] = df_cube[type_name] * df_cube['ratio'] * 1.27 * 0.27
    df_cube = df_cube[['地区', '类型', 'date', 'value']]
    return df_cube


# 电解铝
def process_lv(type_name):
    # 先计算比例
    df_lv = fill_missing(type_name).rename(columns={'value': type_name})
    df_old_lv = df_lv.drop(columns=['year']).groupby(['date']).sum(numeric_only=True).reset_index()
    df_jing_lv = process_quanguo('A02091H01', 'A02091H02').rename(columns={'value': '电解铝'})
    df_ratio = pd.merge(df_old_lv, df_jing_lv)
    df_ratio['ratio'] = df_ratio['电解铝'] / df_ratio[type_name]
    df_ratio = df_ratio[['date', 'ratio']]
    df_lv = pd.merge(df_lv, df_ratio)
    # 计算
    df_lv['value'] = df_lv[type_name] * df_lv['ratio'] * 1.8
    df_lv = df_lv[['地区', '类型', 'date', 'value']]
    return df_lv


def process_all():
    df_gangcai = process_gangcai('钢材')
    df_cement = process_cement('水泥')
    df_cube = process_cube('铜材')
    df_lv = process_lv('铝材')

    df_all = pd.concat([df_gangcai, df_cement, df_cube, df_lv])
    # df_all = df_all[df_all['date'] >= '2019-01-01'].reset_index(drop=True)  # 这里未来要改
    df_all = df_all[df_all['date'] >= '2008-01-01'].reset_index(drop=True)  # 这里未来要改
    df_all['value'] = df_all['value'] / 100  # 万吨到百万吨
    # 汇总
    df_all = df_all.groupby(['地区', 'date', '类型']).sum(numeric_only=True).reset_index().rename(
        columns={'地区': 'city'})
    # 改名
    df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv')).rename(columns={'全称': 'city'})
    df_result = pd.merge(df_all, df_city)[['date', '拼音', 'value', '类型']]
    df_result = df_result.rename(columns={'拼音': 'state'})

    # 按照daily拆分
    df_daily = af.read_daily('Industry')
    df_daily['date'] = pd.to_datetime(df_daily['date'], format='%d/%m/%Y')
    df_daily['year'] = df_daily['date'].dt.year
    df_daily['month'] = df_daily['date'].dt.month
    # 计算月度占比
    df_sum = df_daily.groupby(['year', 'month']).sum(numeric_only=True).reset_index().rename(columns={'value': 'sum'})
    df_ratio = pd.merge(df_daily, df_sum)
    df_ratio['ratio'] = df_ratio['value'] / df_ratio['sum']
    df_ratio = df_ratio[['date', 'year', 'month', 'ratio']]
    # 合并
    df_result['year'] = df_result['date'].dt.year
    df_result['month'] = df_result['date'].dt.month
    df_result = df_result.drop(columns=['date'])
    df_result = pd.merge(df_result, df_ratio)
    df_result['value'] = df_result['value'] * df_result['ratio']

    df_result = df_result[['date', 'state', 'value', '类型']]
    sector_list = df_result['类型'].unique()
    df_result = pd.pivot_table(df_result, index=['date', 'state'], values='value', columns='类型').reset_index()
    df_old = process_old()
    df_old = df_old.drop(columns=['类型'])
    af.out_put(df_old, out_path, 'Industry')
    # df_old = pd.pivot_table(df_old, index=['date', 'state'], values='value', columns='类型').reset_index()
    # df_result = pd.merge(df_result, df_old)
    # df_result['其他'] = df_result['Total'] - df_result[sector_list].sum(axis=1)
    # df_result = df_result.drop(columns=['Total'])
    # # 行转列
    # df_result = df_result.melt(id_vars=['date', 'state'], var_name='类型', value_name='value')  # 更有效率的办法
    # # 输出
    # df_result = df_result.groupby(['date', 'state']).sum(numeric_only=True).reset_index()
    # af.out_put(df_result, out_path, 'Industry')


if __name__ == '__main__':
    main()
