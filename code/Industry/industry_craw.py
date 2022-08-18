# 数据来源 Zhu Deng

from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import time
import requests

requests.packages.urllib3.disable_warnings()
import sys

sys.dont_write_bytecode = True
requests.packages.urllib3.disable_warnings()
sys.path.append('./code/')
from global_code import global_function as af

# 参数
file_path = './data/Industry/craw/'
code_path = os.path.join(file_path, 'code.csv')
out_path = os.path.join(file_path, 'Industry_raw.csv')


def main():
    craw()


def craw():
    # 读取工业各部门代码名称
    code = pd.read_csv(code_path)
    # 读取原始数据
    df_history = pd.read_csv(out_path)
    # 只爬取从下个月开始的数据 #这里暂时不写循环 因为除非1个月不进行爬取 否则不会出错
    max_date = (pd.to_datetime(max(df_history['date']), format='%Y年%m月') + relativedelta(months=1)).strftime('%Y%m')

    url = 'https://data.stats.gov.cn/easyquery.htm'

    headers = af.get_cookie(url)
    code_list = code['code'].tolist()
    name_list = code['cname'].tolist()
    for j, k in zip(code_list, name_list):
        time.sleep(2)  # 不加睡眠会被ban
        keyvalue = {'m': 'QueryData', 'dbcode': 'fsyd', 'rowcode': 'reg', 'colcode': 'sj',
                    'wds': '[{"wdcode":"zb","valuecode":"' + j + '"}]',
                    'dfwds': '[{"wdcode":"sj","valuecode":"%s"}]' % max_date, 'k1': str(int(time.time() * 1000)),
                    'h': 1}

        s = requests.session()
        r = s.get(url, params=keyvalue, headers=headers, timeout=20)
        if r.json()['returncode'] != 501:  # 如果数据更新了再爬
            # 提取数据
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
            df_result['ty'] = k
            # 将结果储存到历史数据里
            df_history = pd.concat([df_history, df_result]).reset_index(drop=True)
    # 输出结果
    df_history = df_history[~df_history.duplicated()].reset_index(drop=True)  # 删除可能存在的重复值
    df_history = df_history.groupby(['name', 'date', 'type']).mean().reset_index()
    df_history.to_csv(out_path, index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
