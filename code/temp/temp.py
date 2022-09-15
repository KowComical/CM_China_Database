import pandas as pd
import os
import requests
import re
import time
import numpy as np

file_path = './data/temp/'
file = os.path.join(file_path, 'result')
df_data = pd.read_csv(os.path.join(file_path, 'result.csv'))


def search_file(file_path):
    import os
    import sys
    sys.dont_write_bytecode = True
    file_name = []
    for parent, surnames, filenames in os.walk(file_path):
        for fn in filenames:
            file_name.append(os.path.join(parent, fn))
    return file_name


# 去掉重复的部分
file_name = search_file(file)
data_list = df_data['upc'].tolist()

data_num = []
name = re.compile(r'result/(?P<name>.*?).csv', re.S)
for f in file_name:
    data_num.append(float(name.findall(f)[0]))
new_list = list(set(data_list) - set(data_num))

# 两部分信息
first_part = re.compile(r'<div class="r col-sm-5 col-md-6 col-xs-12">.*?<ol class="num"><li>(?P<first>.*?)</li>', re.S)
second_part = re.compile(r'<li class="col-xs-12 col-md-6">.*?<a href="/upc/(?P<num>.*?)".*?<p>(?P<name>.*?)</p>', re.S)
for d in new_list[:8000]:
    if [file_name[i] for i, x in enumerate(file_name) if not x.find(str(int(d))) != -1]:  # 如果没下载过再进行操作
        url = 'https://www.upcitemdb.com/query?upc=%s&type=4' % int(d)
        # 开始爬取
        r = requests.get(url)
        # 提取所需要的两端数据并合并
        df_match = pd.DataFrame(first_part.findall(r.text), columns=['match_name'])
        if df_match.empty:  # 如果找不到macth匹配的内容
            df_match = pd.DataFrame([int(d)], columns=['upc'])  # 将upc存入match df中
            df_match['match_name'] = np.nan  # mtachname存入null
        else:
            df_match['upc'] = int(d)
        df_sim = pd.DataFrame(second_part.findall(r.text), columns=['sim_upc', 'sim_name'])  # 存入similar数据
        df_sim['upc'] = int(d)

        df_result = pd.merge(df_match, df_sim)
        # 输出
        df_result.to_csv(os.path.join(file, '%s.csv' % int(d)), index=False, encoding='utf_8_sig')
        time.sleep(1)
