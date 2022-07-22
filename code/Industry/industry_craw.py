#!/usr/bin/env python
# coding: utf-8

# In[3]:


import requests
import pandas as pd
import os
import time

# 参数
path = 'K:\\Github\\中国CM网站\\data\\Industry\\raw'
in_path = os.path.join(path, 'code.csv')
out_path = os.path.join(path, 'Industry_raw.csv')

code = pd.read_csv(in_path)

url = 'https://data.stats.gov.cn/easyquery.htm'

headers = {}

headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) '                         'AppleWebKit/605.1' \
                        '.15 (KHTML, like Gecko) '                         'Version/12.0 Safari/605.1.15 '
headers['Cookie'] = 'u=2; __utmc=64623162; __utmz=64623162.1635922082.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(' \
                    'none); JSESSIONID=nJ3kxv3l1Ujg1oX4U_9IZhL314Q-mHON0X2XUWuWMZf3jDJpxqKs!324916603; ' \
                    '__utma=64623162.61563267.1635922082.1635922082.1635926741.2; __utmt=1; ' \
                    '__utmb=64623162.1.10.1635926741; ' \
                    'wzws_cid' \
                    '=b35ad4a37d61714660b0849077a47a16ba61030151f027cd8709b5f77b' \
                    '0b3c7656e7fd62727b25bd88ad581249b36a41411551f82a298593e81113' \
                    '4e2fe4ed7ddef11d31b5aadf16f73ff8e4e7a5b80a9c355c724918a7d886fa27a6e4eb1916'
headers['Host'] = 'data.stats.gov.cn'
headers['Refer'] = 'https://data.stats.gov.cn/easyquery.htm?cn=A01'

code_list = code['code'].tolist()
name_list = code['cname'].tolist()
for j, k in zip(code_list, name_list):
    keyvalue = {}

    keyvalue['m'] = 'QueryData'
    keyvalue['dbcode'] = 'fsyd'
    keyvalue['rowcode'] = 'reg'
    keyvalue['colcode'] = 'sj'
    keyvalue['wds'] = '[{"wdcode":"zb","valuecode":"' + j + '"}]'
    keyvalue['dfwds'] = '[{"wdcode":"sj","valuecode":"2019-2022"}]'
    keyvalue['k1'] = str(int(time.time() * 1000))
    keyvalue['h'] = 1

    s = requests.session()
    r = s.get(url, params=keyvalue, headers=headers, verify=False)
    time.sleep(2)

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

    if os.path.exists(out_path):
        df_result.to_csv(out_path, mode='a', header=False, index=False, encoding='utf_8_sig')
    else:
        df_result.to_csv(out_path, index=False, encoding='utf_8_sig')
