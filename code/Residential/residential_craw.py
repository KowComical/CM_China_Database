import pandas as pd
import os
from sklearn.linear_model import LinearRegression
from datetime import datetime

import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af

global_path = './data/'
raw_path = os.path.join(global_path, 'Residential', 'raw')
craw_path = os.path.join(global_path, 'Residential', 'craw')
useful_path = os.path.join(global_path, 'global_data')


def main():
    craw()


def craw():
    # 设置爬取范围
    end_year = datetime.now().strftime('%Y')
    date_range = '2004-%s' % end_year
    # 爬取数据
    df_result = af.get_json('fsnd', 'A0B0507', date_range)
    # 输出raw数据
    df_result.to_csv(os.path.join(craw_path, '供热面积.csv'), index=False, encoding='utf_8_sig')
    # 继续清理
    df_result['data'] = df_result['data'] * 10000  # 亿平方米转为万立方迷
    df_result['date'] = df_result['date'].str.replace('年', '').astype(int)

    # 线性填充缺失的供热面积
    # 如果一年各省份数据都一致 则代表是错误数据 需要删掉
    # 列转行
    df_result = pd.pivot_table(df_result, index='name', values='data', columns='date').reset_index()
    for d in df_result.columns:
        if (df_result[d] == df_result[d][0]).all():  # 如果某一列的第一个数等于整列的数
            df_result = df_result.drop(columns=d)  # 则删掉
    # 行转列
    df_result = df_result.set_index(['name']).stack().reset_index().rename(columns={'level_1': 'date', 0: 'data'})

    province_list = df_result['name'].unique()
    start_year = max(df_result['date']) + 1
    df_predicted = pd.DataFrame()
    for p in province_list:
        temp = df_result[df_result['name'] == p].reset_index(drop=True)
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
            predict['name'] = p
            temp = pd.concat([temp, predict]).reset_index(drop=True)
            df_predicted = pd.concat([df_predicted, temp]).reset_index(drop=True)

    # 将所有预测的不好的改为0
    df_null = df_predicted[df_predicted['data'] < 0].reset_index(drop=True)
    df_null['data'] = 0
    df_rest = df_predicted[df_predicted['data'] >= 0].reset_index(drop=True)
    df_all = pd.concat([df_null, df_rest]).reset_index(drop=True)
    # 改成英文名
    df_city = pd.read_csv(os.path.join(useful_path, 'city_name.csv'))
    df_all = pd.merge(df_all, df_city, left_on='name', right_on='全称')[['拼音', 'date', 'data']].rename(
        columns={'拼音': 'Provinces'})
    # 列转行
    df_all = pd.pivot_table(df_all, index='Provinces', values='data', columns='date').reset_index()
    # 输出
    df_all.to_csv(os.path.join(raw_path, 'ratio.csv'), index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
