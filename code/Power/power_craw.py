# 数据来源 Zhu Deng
import requests
import pandas as pd
import os
import time
import re
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

requests.packages.urllib3.disable_warnings()

# 参数
file_path = './data/Power/craw/'
end_year = datetime.now().strftime('%Y')

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
options.add_argument('--ignore-certificate-errors')


def main():
    craw()


def craw():
    # 借用selenium爬 requests的办法因为ip被封没法用了
    for k in ['A03010H01', 'A03010H02']:
        # 将参数直接添加到网页中来爬
        dbcode_value = 'fsyd'
        wds_value = '[{"wdcode":"zb","valuecode":"%s"}]' % k
        dfwds_value = '[{"wdcode":"sj","valuecode": "2019-%s"}]' % end_year
        k1_value = str(int(time.time() * 1000))
        url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=%s&rowcode=reg&colcode=sj&wds=%s&dfwds=%s' \
              '&k1=%s' % (dbcode_value, wds_value, dfwds_value, k1_value)
        # 开始爬
        wd = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)  # 打开浏览器
        wd.get(url)
        # 得到网页源码
        content = wd.page_source
        # 提取有效数据
        json_data = re.compile(r'pre-wrap;">(?P<data>.*?)</pre>', re.S)
        result = json.loads(json_data.findall(content)[0])
        # 清理并输出
        name = []
        date = []
        data = []
        city_list = pd.json_normalize(result['returndata']['wdnodes'][1], record_path='nodes')['name'].tolist()
        time_list = pd.json_normalize(result['returndata']['wdnodes'][2], record_path='nodes')['name'].tolist()
        for c in city_list:
            for t in time_list:
                name.append(c)
                date.append(t)
        data_list = result['returndata']['datanodes']
        for i in range(len(data_list)):
            data.append(pd.DataFrame([data_list[i]['data']])['data'].tolist()[0])
        df_result = pd.concat([pd.DataFrame(name, columns=['name']), pd.DataFrame(date, columns=['date']),
                               pd.DataFrame(data, columns=['data'])], axis=1)
        if k == 'A03010H01':
            df_result.to_csv(os.path.join(file_path, '火电当月.csv'), index=False, encoding='utf_8_sig')
        else:
            df_result.to_csv(os.path.join(file_path, '火电累计.csv'), index=False, encoding='utf_8_sig')
        wd.quit()


if __name__ == '__main__':
    main()
