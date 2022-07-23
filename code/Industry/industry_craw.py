# 数据来源 Zhu Deng

from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import time
import requests

requests.packages.urllib3.disable_warnings()

# 参数
file_path = 'K:\\Github\\CM_China_Database\\data\\Industry\\craw\\'
code_path = os.path.join(file_path, 'code.csv')
out_path = os.path.join(file_path, 'Industry_raw.csv')
# 读取工业各部门代码名称
code = pd.read_csv(code_path)
# 读取原始数据
df_history = pd.read_csv(out_path)
# 只爬取从下个月开始的数据 #这里暂时不写循环 因为除非1个月不进行爬取 否则不会出错
max_date = (pd.to_datetime(max(df_history['date']), format='%Y年%m月') + relativedelta(months=1)).strftime('%Y%m')

url = 'https://data.stats.gov.cn/easyquery.htm'

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) '                         'AppleWebKit/605.1'
                         '.15 (KHTML, like Gecko) '                         'Version/12.0 Safari/605.1.15 ',
           'Cookie': 'u=6; '
                     'wzws_cid'
                     '=e8dd55e9229bc223d01d4e0f49004bcf5673511c1a0e2b62c3be28b18c8cc3d2f901dff2244ab6415125cea22b'
                     '253ee3ae79febce318639f6be642bdb9c6b1b07d1d6d13c76bc68cee8a83ff40ae5119; '
                     'JSESSIONID=N74rWoHGztsgiVi9 '
                     'yZlFckhk7X2v1DEWnBzFadKR1bXHR3OW_S0X!2063508790',
           'Host': 'data.stats.gov.cn', 'Refer': 'https://data.stats.gov.cn/easyquery.htm?cn=A01'}

code_list = code['code'].tolist()
name_list = code['cname'].tolist()
for j, k in zip(code_list, name_list):
    time.sleep(2)  # 不加睡眠会被ban
    keyvalue = {'m': 'QueryData', 'dbcode': 'fsyd', 'rowcode': 'reg', 'colcode': 'sj',
                'wds': '[{"wdcode":"zb","valuecode":"' + j + '"}]',
                'dfwds': '[{"wdcode":"sj","valuecode":"%s"}]' % max_date, 'k1': str(int(time.time() * 1000)), 'h': 1}

    s = requests.session()
    r = s.get(url, params=keyvalue, headers=headers, verify=False)
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
