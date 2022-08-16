import requests
import os
from datetime import datetime
import time
import pandas as pd
from sklearn.linear_model import LinearRegression

import sys

sys.dont_write_bytecode = True

global_path = './data/'
raw_path = os.path.join(global_path, 'Ground_Transport', 'raw')
craw_path = os.path.join(global_path, 'Ground_Transport', 'craw')
useful_path = os.path.join(global_path, 'global_data')

end_year = datetime.now().strftime('%Y')

url = 'https://data.stats.gov.cn/easyquery.htm'

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) '                         'AppleWebKit/605.1'
                  '.15 (KHTML, like Gecko) '                         'Version/12.0 Safari/605.1.15 ',
    'Host': 'data.stats.gov.cn', 'Refer': 'https://data.stats.gov.cn/easyquery.htm?cn=E0102'}
keyvalue = {'m': 'QueryData', 'dbcode': 'fsnd', 'rowcode': 'reg', 'colcode': 'sj',
            'wds': '[{"wdcode":"zb","valuecode":"A0G0701"}]',
            'dfwds': '[{"wdcode":"sj","valuecode":"1980-%s"}]' % end_year, 'k1': str(int(time.time() * 1000))}


def main():
    craw()


def craw():
    r = requests.get(url, params=keyvalue, headers=headers, timeout=20)
    name = []
    date = []
    data = []
    city_list = pd.json_normalize(r.json()['returndata']['wdnodes'][1], record_path='nodes')['name'].tolist()
    time_list = pd.json_normalize(r.json()['returndata']['wdnodes'][2], record_path='nodes')['name'].tolist()
    for c in city_list:
        for t in time_list:
            name.append(c)
            date.append(t)
    data_list = r.json()['returndata']['datanodes']
    for i in range(len(data_list)):
        data.append(pd.DataFrame([data_list[i]['data']])['data'].tolist()[0])
    df_result = pd.concat([pd.DataFrame(name, columns=['name']), pd.DataFrame(date, columns=['date']),
                           pd.DataFrame(data, columns=['data'])], axis=1)

    # 线性填充缺失的汽车保有量
    df_result = pd.pivot_table(df_result, index='name', values='data', columns='date').reset_index()
    for d in df_result.columns:
        if (df_result[d] == df_result[d][0]).all():  # 如果某一列的第一个数等于整列的数
            df_result = df_result.drop(columns=d)  # 则删掉
    # 行转列
    df_result = df_result.set_index(['name']).stack().reset_index().rename(columns={'level_1': 'date', 0: 'data'})
    # 读取历史数据
    df_history = pd.read_csv(os.path.join(craw_path, '汽车保有量.csv'))
    # 合并新旧数据
    df_result = pd.concat([df_history, df_result]).reset_index(drop=True)
    # 删除重复值
    df_result = df_result.groupby(['name', 'date', 'data']).mean().reset_index()
    # 输出raw
    df_result.to_csv(os.path.join(craw_path, '汽车保有量.csv'), index=False, encoding='utf_8_sig')
    # 继续清理
    df_result['date'] = df_result['date'].str.replace('年', '').astype(int)
    df_result = df_result[df_result['date'] >= 2010].reset_index(drop=True)  # 以为多了年份预测会更好 结果更差 不知道为啥
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
        columns={'拼音': 'Province'})
    # 列转行
    df_all = pd.pivot_table(df_all, index='Province', values='data', columns='date').reset_index()
    df_all.to_csv(os.path.join(raw_path, 'ratio.csv'), index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
