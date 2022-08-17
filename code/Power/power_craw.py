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

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/104.0.0.0 Safari/537.36',
           'Host': 'data.stats.gov.cn', 'Refer': 'https://data.stats.gov.cn/easyquery.htm?cn=A01',
           'Cookie': '_trs_uv=l64kf5g2_6_ajxr; JSESSIONID=17GrWn_4QlmhKwdwo3ffZyJ76pR54oBiWMZELhpFhPplVcNbm6PB'
                     '!2063508790; u=6; experience=show'}
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
