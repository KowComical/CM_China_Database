import pandas as pd
import os
from datetime import datetime
from sklearn.linear_model import LinearRegression

import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af
from Power import power_craw as pc

global_path = './data/'
raw_path = os.path.join(global_path, 'Power', 'raw')
craw_path = os.path.join(global_path, 'Power', 'craw')
useful_path = os.path.join(global_path, 'global_data')
out_path = os.path.join(global_path, 'Power', 'cleaned')

end_year = datetime.now().strftime('%Y')


def main():
    pc.main()
    process()


def process():
    # 读取daily数据 并计算占比
    df_daily = af.read_daily(useful_path, 'Power')
    df_daily['date'] = pd.to_datetime(df_daily['date'], format='%d/%m/%Y')
    df_daily['year'] = df_daily['date'].dt.year
    df_daily['month'] = df_daily['date'].dt.month
    df_sum = df_daily.groupby(['year', 'month']).sum().reset_index().rename(columns={'value': 'sum'})
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
    df_dangqi = df_dangqi.rename(columns={'name': '地区'})
    df_dangqi = df_dangqi.set_index(['地区']).stack().reset_index().rename(columns={'level_1': 'date', 0: 'value'})
    df_dangqi['date'] = pd.to_datetime(df_dangqi['date'], format='%Y年%m月')
    df_dangqi['year'] = df_dangqi['date'].dt.year
    df_dangqi = df_dangqi.rename(columns={'地区': 'state'})
    df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv')).rename(columns={'全称': 'state'}).drop(
        columns=['地区'])
    df_dangqi = pd.merge(df_dangqi, df_city)[['date', 'year', '拼音', 'value']].rename(columns={'拼音': 'state'})

    # 乘以排放因子
    df_ef = predict_ef()
    df_ef = df_ef.set_index(['省级电网']).stack().reset_index().rename(columns={'level_1': 'date', 0: 'ef'}).rename(
        columns={'省级电网': 'state'})

    df_ef = pd.merge(df_ef, df_city, left_on='state', right_on='中文')[['date', '拼音', 'ef']].rename(
        columns={'拼音': 'state', 'date': 'year'})

    df_result = pd.merge(df_dangqi, df_ef)
    df_result['value'] = df_result['value'] * df_result['ef'] * 0.1  # 将所有值都乘以0.1 保持单位一致
    df_result['month'] = df_result['date'].dt.month
    df_result = df_result.drop(columns=['ef', 'date'])

    # # 将结果拆分到daily
    df_result = pd.merge(df_result, df_ratio)
    df_result['value'] = df_result['value'] * df_result['ratio']
    df_result = df_result[['date', 'state', 'value']]

    # 输出
    af.out_put(df_result, out_path, 'Power')


def predict_ef():
    df = pd.read_csv(os.path.join(raw_path, 'ef.csv'))
    df = df.set_index(['省级电网']).stack().reset_index().rename(columns={'level_1': 'date', 0: 'data'})
    df['date'] = 2000 + df['date'].str.replace('年排放因子', '').astype(int)
    df_result = df.copy()
    province_list = df_result['省级电网'].unique()
    start_year = max(df_result['date']) + 1
    df_predicted = pd.DataFrame()
    for p in province_list:
        temp = df_result[df_result['省级电网'] == p].reset_index(drop=True)
        # 线性填充
        for i in range(start_year, int(end_year) + 1):
            X = temp['date'].values.reshape(-1, 1)  # put your dates in here
            y = temp['data'].values.reshape(-1, 1)  # put your kwh in here

            model = LinearRegression()
            model.fit(X, y)

            X_predict = pd.DataFrame([i]).values.reshape(-1, 1)  # put the dates of which you want to predict kwh here
            y_predict = model.predict(X_predict)

            # 将结果加入df中
            predict = pd.DataFrame([[int(X_predict), float(y_predict)]], columns=temp.columns[1:])
            predict['省级电网'] = p
            temp = pd.concat([temp, predict]).reset_index(drop=True)
            df_predicted = pd.concat([df_predicted, temp]).reset_index(drop=True)

    # 将所有预测的不好的改为0
    df_null = df_predicted[df_predicted['data'] < 0].reset_index(drop=True)
    df_null['data'] = 0
    df_rest = df_predicted[df_predicted['data'] >= 0].reset_index(drop=True)
    df_all = pd.concat([df_null, df_rest]).reset_index(drop=True)
    # 列转行
    df_all = pd.pivot_table(df_all, index='省级电网', values='data', columns='date').reset_index()
    return df_all


if __name__ == '__main__':
    main()
