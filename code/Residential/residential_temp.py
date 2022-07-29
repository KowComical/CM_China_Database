import requests
import os
from datetime import datetime
import time
import pandas as pd

file_path = 'K:\\Github\\CM_China_Database\\data\\Residential\\craw\\'
end_year = datetime.now().strftime('%Y')

url = 'https://data.stats.gov.cn/easyquery.htm'

sector_list = ['A03010M01', 'A03010M02', 'A03010501', 'A03010502', 'A03010C01', 'A03010C02', 'A070401', 'A070405']
name_list = ['煤气当期', '煤气累计', '液化天然气当期', '液化天然气累计', '液化石油气当期', '液化石油气累计',
             '城市天然气', '石油液化气']  # 话说这个城市天然气和石油液化气更新频率太低了吧？2022年7月30 2021年的数据还没有 要不去掉吧？
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) '                         'AppleWebKit/605.1'
                  '.15 (KHTML, like Gecko) '                         'Version/12.0 Safari/605.1.15 ',
    'Host': 'data.stats.gov.cn', 'Refer': 'https://data.stats.gov.cn/easyquery.htm?cn=E0102'}


def main():
    craw()


def craw():
    for s, n in zip(sector_list, name_list):
        time.sleep(2)  # 不加睡眠会被ban
        freq = 'fsyd'  # 分省月度
        if s == 'A070401' or s == 'A070405':  # 城市天然气和石油液化气只有年度数据
            freq = 'fsnd'  # 分省年度
        keyvalue = {'m': 'QueryData', 'dbcode': freq, 'rowcode': 'reg', 'colcode': 'sj',
                    'wds': '[{"wdcode":"zb","valuecode":"%s"}]' % s,
                    'dfwds': '[{"wdcode":"sj","valuecode":"%s"}]' % end_year, 'k1': str(int(time.time() * 1000))}

        # 提取数据
        r = requests.get(url, params=keyvalue, headers=headers, verify=False)
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
        # 按照名称读取各自的历史数据
        df_history = pd.read_csv(os.path.join(file_path, '%s.csv' % n))
        # 新旧合并
        df_result = pd.concat([df_result, df_history]).reset_index(drop=True)
        df_result = df_result.groupby(['name', 'date']).mean().reset_index()  # 去掉重复值
        # 输出
        df_result.to_csv(os.path.join(file_path, '%s.csv' % n), index=False, encoding='utf_8_sig')


def ratio_1():
    file_name = af.search_file(craw_path)
    # 读取所有当期和累计
    dangqi_path = [file_name[i] for i, x in enumerate(file_name) if x.find('当') != -1]
    leiji_path = [file_name[i] for i, x in enumerate(file_name) if x.find('累计') != -1]
    # 提取名称
    name = re.compile(r'craw\\(?P<name>.*?)当期', re.S)
    # 填充当期缺失值
    # 读取工作日
    work = pd.read_csv(os.path.join(useful_path, 'workday.csv'))
    work['date'] = work['year'].astype(str) + '年' + work['month'].astype(str) + '月'

    for dangqi, leiji in zip(dangqi_path, leiji_path):
        df_dangqi = pd.read_csv(dangqi)
        df_leiji = pd.read_csv(leiji)

        df_dangqi = pd.pivot_table(df_dangqi, index='name', values='data', columns='date').reset_index()
        df_leiji = pd.pivot_table(df_leiji, index='name', values='data', columns='date').reset_index()

        # 补全缺失的1&2月当月值
        min_year = int(min(df_dangqi.columns[1:])[:4])
        max_year = int(max(df_dangqi.columns[1:])[:4])
        for i in range(min_year, max_year + 1):  # 按照当前的年份的最小值和最大值来填充1月和2月数据
            df_dangqi['%s年1月' % i] = df_leiji['%s年2月' % i] * work[work['date'] == '%s年1月' % i]['ratio'].values
            df_dangqi['%s年2月' % i] = df_leiji['%s年2月' % i] * work[work['date'] == '%s年2月' % i]['ratio'].values

        # 计算每月省份占全国比例
        df_dangqi = df_dangqi.set_index(['name']).stack().reset_index().rename(
            columns={'level_2': 'date', 0: 'value'})
        df_sum = df_dangqi.groupby(['date']).sum().reset_index().rename(columns={'value': 'sum'})
        df_ratio = pd.merge(df_dangqi, df_sum)
        df_ratio['ratio'] = df_ratio['value'] / df_ratio['sum']
        df_ratio = df_ratio[['name', 'date', 'ratio']]
        # 列转行
        df_ratio = pd.pivot_table(df_ratio, index='name', values='ratio', columns='date').reset_index()
        # 输出
        df_ratio.to_csv(os.path.join(raw_path, '%s_省份月度占比.csv' % name.findall(dangqi)[0]), index=False,
                        encoding='utf_8_sig')
        df_ratio.to_csv(os.path.join(raw_path, '%s.csv' % name.findall(dangqi)[0]), index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
