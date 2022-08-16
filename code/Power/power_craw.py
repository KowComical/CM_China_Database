# 数据来源 Zhu Deng
import requests
import pandas as pd
import os
import time
from datetime import datetime

requests.packages.urllib3.disable_warnings()

# 参数
file_path = './data/Power/craw/'
end_year = datetime.now().strftime('%Y')

url = 'https://data.stats.gov.cn/easyquery.htm'

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) '                         'AppleWebKit/605.1'
                         '.15 (KHTML, like Gecko) '                         'Version/12.0 Safari/605.1.15 ',
           'Cookie': 'u=6; '
                     'wzws_cid'
                     '=e8dd55e9229bc223d01d4e0f49004bcf5673511c1a0e2b62c3be28b18c8cc3d2f901dff2244ab6415125'
                     'cea22b253ee3ae79febce318639f6be642bdb9c6b1b07d1d6d13c76bc68cee8a83ff40ae5119; JSESSIONID=N74'
                     'rWoHGztsgiVi9yZlFckhk7X2v1DEWnBzFadKR1bXHR3OW_S0X!2063508790',
           'Host': 'data.stats.gov.cn', 'Refer': 'https://data.stats.gov.cn/easyquery.htm?cn=A01'}

keyvalue = {'m': 'QueryData', 'dbcode': 'fsyd', 'rowcode': 'reg', 'colcode': 'sj'}


def main():
    craw()


def craw():
    for k in ['A03010H01', 'A03010H02']:
        keyvalue['wds'] = '[{"wdcode":"zb","valuecode":"%s"}]' % k
        keyvalue['dfwds'] = '[{"wdcode":"sj","valuecode":"2019-%s"}]' % end_year
        keyvalue['k1'] = str(int(time.time() * 1000))
        keyvalue['h'] = 1

        s = requests.session()
        r = s.get(url, params=keyvalue, headers=headers, timeout=20)

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
        if k == 'A03010H01':
            df_result.to_csv(os.path.join(file_path, '火电当月.csv'), index=False, encoding='utf_8_sig')
        else:
            df_result.to_csv(os.path.join(file_path, '火电累计.csv'), index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
