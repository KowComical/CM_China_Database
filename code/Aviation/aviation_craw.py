# 数据来源 Zhu Deng
import re
import pandas as pd
import os
import requests
import pdfplumber
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af

file_path = './data/Aviation/craw/'
out_path = os.path.join(file_path, '生产资料')

# 设置备注
options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
options.add_argument('--headless')
options.add_argument('--disable-gpu')


def main():
    craw()
    extract_pdf()


def craw():
    # 打开网页
    url = 'http://www.caac.gov.cn/so/s?qt=中国民航%20月份主要生产指标统计.pdf&siteCode=bm70000001&tab=xxgk&toolsStatus=1'
    wd = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)  # 打开浏览器
    wd.get(url)  # 打开要爬的网址
    wd.implicitly_wait(60)
    html = wd.page_source

    # 提取有效信息
    name = re.compile(r'<div style="float: right">.*?<a record="false" href="(?P<name>.*?)"'
                      r'.*?class="fontlan" title="(?P<title>.*?).pdf"', re.S)
    df_result = pd.DataFrame(name.findall(html), columns=['url', 'name'])
    df_result['name'] = df_result['name'].str.replace(
        '中国民航', '').str.replace(
        '主要生产指标统计', '').str.replace(
        '主要运输生产指标统计', '').str.replace(
        '民航', '').str.replace('4国', '').str.replace('份', '').str.replace(' ', '').str.replace('"', '')
    df_result['name'] = df_result['name'].replace(df_result['name'].tolist()[-1], df_result['name'].tolist()[-1][:8])
    df_result['date'] = pd.to_datetime(df_result['name'], format='%Y年%m月').dt.strftime('%Y-%m')
    df_result['name'] = pd.to_datetime(df_result['date']).dt.strftime('%Y年%m月航空生产资料')

    # 读取history文件
    df_history = pd.read_csv(os.path.join(file_path, '全生产资料信息.csv'))
    df_result = pd.concat([df_result, df_history]).reset_index(drop=True)
    df_result = df_result[~df_result.duplicated()].reset_index(drop=True)
    df_result.to_csv(os.path.join(file_path, '全生产资料信息.csv'), index=False, encoding='utf_8_sig')

    # 找到已下载的
    file_name = af.search_file(out_path)
    # 下载未下载的
    name_list = df_result['name'].tolist()
    path_list = df_result['url'].tolist()
    for n, p in zip(name_list, path_list):
        if not [file_name[i] for i, x in enumerate(file_name) if x.find(n) != -1]:  # 如果未下载
            r = requests.get(p)
            with open(os.path.join(out_path, '%s.pdf' % n), 'wb') as f:
                f.write(r.content)


def extract_pdf():
    # 转换整理的pdf
    file_name = af.search_file(out_path)
    name = re.compile(r'生产资料/(?P<name>.*?)航空生产资料', re.S)
    df_result = pd.DataFrame()
    for f in file_name:
        try:
            pdf = pdfplumber.open(f)
            dfs = pdf.pages[0].extract_tables()
            temp = pd.DataFrame(dfs[0]).fillna(np.nan).replace('', np.nan).replace('\n', '')
            temp = temp.dropna(axis=0, how='all', thresh=2).reset_index(drop=True)  # 非空值小于2时删除行
            temp = temp.dropna(axis=1, how='all', thresh=4)
            # 提取有效信息范围
            start_index = temp[temp[0] == '旅客吞吐量'].index.tolist()[0]
            temp = temp.iloc[start_index:start_index + 5, :3].reset_index(drop=True)
            temp.columns = ['region', 'unit', 'value']
            temp['date'] = name.findall(f)[0]
            df_result = pd.concat([df_result, temp]).reset_index(drop=True)
        except:
            pass
    # 列转行
    df_result = pd.pivot_table(df_result, index=['date', 'unit'], values='value', columns='region').reset_index()
    # 输出
    df_result.to_csv(os.path.join(file_path, '各地区旅客吞吐量.csv'), index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
