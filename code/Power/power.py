import pandas as pd
import os
from sklearn.linear_model import LinearRegression

import sys

env_path = '/data3/kow/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af
from Power import power_craw as pc

global_path, raw_path, craw_path, useful_path, out_path = af.useful_element('Power')


def main():
    pc.main()
    process()


def process():
    # 读取daily数据 并计算占比
    df_daily = af.read_daily('Power')

    df_daily['year'] = df_daily['date'].dt.year
    df_daily['month'] = df_daily['date'].dt.month
    df_sum = df_daily.groupby(['year', 'month']).sum(numeric_only=True).reset_index().rename(columns={'value': 'sum'})
    df_ratio = pd.merge(df_daily, df_sum)
    df_ratio['ratio'] = df_ratio['value'] / df_ratio['sum']
    df_ratio = df_ratio[['year', 'month', 'ratio', 'date']]

    # 读取当期和累计数据
    file_name = af.search_file(craw_path)
    dangqi_path = [file_name[i] for i, x in enumerate(file_name) if x.find('当月') != -1][0]
    leiji_path = [file_name[i] for i, x in enumerate(file_name) if x.find('累计') != -1][0]

    df_dangqi = pd.read_csv(dangqi_path)
    df_leiji = pd.read_csv(leiji_path)

    df_dangqi = pd.pivot_table(df_dangqi, index='name', values='data', columns='date').reset_index()
    df_leiji = pd.pivot_table(df_leiji, index='name', values='data', columns='date').reset_index()
    # 去除还没有2月的1月
    df_dangqi = af.find_missing_month(df_dangqi)
    df_leiji = af.find_missing_month(df_leiji)

    # 读取工作日
    work = af.process_workday()
    # 补全缺失的1&2月当月值
    min_year = int(min(df_dangqi.columns[1:])[:4])
    max_year = int(max(df_dangqi.columns[1:])[:4])
    for year in range(min_year, max_year + 1):  # 按照当前的年份的最小值和最大值来填充1月和2月数据
        # 定义1月和2月的变量
        jan_month = f'{year}年1月'
        feb_month = f'{year}年2月'

        df_dangqi[jan_month] = df_leiji[feb_month] * work[work['date'] == jan_month]['ratio'].values
        df_dangqi[feb_month] = df_leiji[feb_month] * work[work['date'] == feb_month]['ratio'].values
    # 整理当期数据
    df_dangqi = df_dangqi.rename(columns={'name': '地区'})
    df_dangqi = df_dangqi.set_index(['地区']).stack().reset_index().rename(columns={'level_1': 'date', 0: 'value'})
    df_dangqi['date'] = pd.to_datetime(df_dangqi['date'], format='%Y年%m月')
    df_dangqi['year'] = df_dangqi['date'].dt.year
    df_dangqi = df_dangqi.rename(columns={'地区': 'state'})
    df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv')).rename(columns={'全称': 'state'}).drop(
        columns=['地区'])
    df_dangqi = pd.merge(df_dangqi, df_city)[['date', 'year', '拼音', 'value']].rename(columns={'拼音': 'state'})

    # 乘以排放因子
    max_year = max(df_dangqi['year'])
    df_ef = predict_ef(max_year)

    df_ef = pd.merge(df_ef, df_city, left_on='省级电网', right_on='中文')[['date', '拼音', 'data']].rename(
        columns={'拼音': 'state', 'date': 'year', 'data': 'ef'})

    df_result = pd.merge(df_dangqi, df_ef).sort_values(by='date')
    df_result['value'] = df_result['value'] * df_result['ef'] * 0.1  # 将所有值都乘以0.1 保持单位一致
    df_result['month'] = df_result['date'].dt.month
    df_result = df_result.drop(columns=['ef', 'date'])

    # # 将结果拆分到daily
    df_result = pd.merge(df_result, df_ratio)
    df_result['value'] = df_result['value'] * df_result['ratio']
    df_result = df_result[['date', 'state', 'value']]

    # 输出
    af.out_put(df_result, out_path, 'Power')


def predict_ef(max_year):
    df = pd.read_csv(os.path.join(raw_path, 'ef.csv'))
    df = df.set_index(['省级电网']).stack().reset_index().rename(columns={'level_1': 'date', 0: 'data'})
    df['date'] = 2000 + df['date'].str.replace('年排放因子', '').astype(int)

    province_list = df['省级电网'].unique()
    start_year = max(df['date']) + 1

    df_temp = []
    for p in province_list:
        temp = df[df['省级电网'] == p].reset_index(drop=True)
        # 线性填充
        for year in range(start_year, max_year + 1):
            X = temp['date'].values.reshape(-1, 1)  # 日期数据
            y = temp['data'].values.reshape(-1, 1)  # 电力数据

            model = LinearRegression()
            model.fit(X, y)

            X_predict = pd.DataFrame([year]).values.reshape(-1, 1)  # 需要预测的日期
            y_predict = model.predict(X_predict)

            # 提取 y_predict 中的单个元素，并正确转换为 float
            y_predict_single = float(y_predict[0][0])

            # 将预测结果添加到 DataFrame
            predict = pd.DataFrame([[year, y_predict_single]], columns=['date', 'data'])
            predict['省级电网'] = p  # 添加省份标识
            df_temp.append(predict)

    df_predicted = pd.concat(df_temp).reset_index(drop=True)

    # 将所有预测的不好的改为0
    df_null = df_predicted[df_predicted['data'] < 0].reset_index(drop=True)
    df_null['data'] = 0
    df_rest = df_predicted[df_predicted['data'] >= 0].reset_index(drop=True)
    df_all = pd.concat([df_null, df_rest]).reset_index(drop=True)
    # 和旧的合并
    df_final = pd.concat([df, df_all]).reset_index(drop=True)
    df_final = df_final.drop_duplicates(subset=['省级电网', 'date'])

    return df_final


if __name__ == '__main__':
    main()
