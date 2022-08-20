# 数据来源 Zhu Deng
import re
import pandas as pd
import os
import requests

import pdfplumber
from datetime import datetime
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af

global_path = './data/'
aviation_path = os.path.join(global_path, 'Aviation')
file_path = os.path.join(aviation_path, 'craw')
out_path = os.path.join(file_path, '生产资料')
out_path_gdp = os.path.join(file_path, 'GDP.csv')
raw_path = os.path.join(aviation_path, 'raw')

# 设置备注
options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
options.add_argument('--ignore-certificate-errors')


def main():
    craw_zhibiao()  # 爬取航空指标数据
    extract_pdf()  # 将pdf转为数据
    craw_gdp()  # 爬取gdp数据
    gdp_raw()  # 将gdp整理输出到raw
    ratio()  # 计算所需占比


def craw_zhibiao():
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
    df_result['date'] = pd.to_datetime(df_result['name'], format='%Y年%m月').dt.strftime('%Y-%m')
    df_result['name'] = pd.to_datetime(df_result['date']).dt.strftime('%Y年%m月航空生产资料')

    # 读取history文件
    df_history = pd.read_csv(os.path.join(file_path, '全生产资料信息.csv'))
    df_result = pd.concat([df_result, df_history]).reset_index(drop=True)
    df_result = df_result[~df_result.duplicated(['url'])].reset_index(drop=True)
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
    wd.quit()


def extract_pdf():
    # 转换整理的pdf
    file_name = af.search_file(out_path)
    name = re.compile(r'生产资料/(?P<name>.*?)航空生产资料', re.S)
    df_result = pd.DataFrame()
    for f in file_name:
        pdf = pdfplumber.open(f)
        dfs = pdf.pages[0].extract_tables()
        temp = pd.DataFrame(dfs[0]).fillna(np.nan).replace('', np.nan).replace('\n', '')
        if '旅客吞吐量' in temp[0].tolist():  # 如果有旅客吞吐量
            temp = temp.dropna(axis=0, how='all', thresh=2).reset_index(drop=True)  # 非空值小于2时删除行
            temp = temp.dropna(axis=1, how='all', thresh=4)
            # 提取有效信息范围
            start_index = temp[temp[0] == '旅客吞吐量'].index.tolist()[0]
            temp = temp.iloc[start_index:start_index + 5, :3].reset_index(drop=True)
            temp.columns = ['region', 'unit', 'value']
            temp['date'] = name.findall(f)[0]
            df_result = pd.concat([df_result, temp]).reset_index(drop=True)
    # 列转行
    df_result = pd.pivot_table(df_result, index=['date', 'unit'], values='value', columns='region').reset_index()
    df_result = df_result.rename(columns={'其中：东部地区': '东部地区'}).drop(columns=['旅客吞吐量'])
    # 行转列
    df_result = df_result.set_index(['date', 'unit']).stack().reset_index().rename(
        columns={'level_2': '地区', 0: 'zhibiao'})
    # 输出
    df_result.to_csv(os.path.join(file_path, '各地区旅客吞吐量.csv'), index=False, encoding='utf_8_sig')


def craw_gdp():
    # 参数
    end_year = datetime.now().strftime('%Y')
    # 爬取
    df_result = af.get_json('fsjd', 'A010101', end_year)
    # 读取历史数据
    df_history = pd.read_csv(out_path_gdp)
    # 合并结果并删除重复值
    df_result = pd.concat([df_result, df_history]).reset_index(drop=True)
    df_result = df_result.groupby(['name', 'date']).mean().reset_index()
    # 输出结果
    df_result.to_csv(out_path_gdp, index=False, encoding='utf_8_sig')


def gdp_raw():
    df_city = pd.read_csv(os.path.join(global_path, 'global_data', 'city_name.csv'))
    df_gdp = pd.read_csv(out_path_gdp)

    # 将所有中文季度转为日期格式
    q_list = ['第一季度', '第二季度', '第三季度', '第四季度']
    num_list = ['01', '04', '07', '10']
    for q, d in zip(q_list, num_list):
        df_gdp['date'] = df_gdp['date'].str.replace('年%s' % q, '-%s' % d)

        # 将季度分成月份
    province_list = df_gdp['name'].unique()
    df_all = pd.DataFrame()
    for p in province_list:
        temp = df_gdp[df_gdp['name'] == p].reset_index(drop=True)
        df_result = pd.DataFrame()
        for q, d in zip(q_list, num_list):
            temp['date'] = temp['date'].str.replace('年%s' % q, '-%s' % d)  # 将所有中文季度转为日期格式
            date_list = temp['date'].unique()  # 生成当前省份的日期列表
            df_province = pd.DataFrame()
            for date in date_list:
                df_date = pd.DataFrame()
                date_range = pd.date_range(start=date, periods=3, freq='M').strftime("%Y-%m")
                next_temp = temp[temp['date'] == date].reset_index(drop=True)
                df_date['date'] = date_range
                df_date['value'] = next_temp['data'].tolist()[0] / 3  # 将季度数据平分到月度
                df_date['province'] = p
                df_province = pd.concat([df_province, df_date]).reset_index(drop=True)
            df_result = pd.concat([df_result, df_province]).reset_index(drop=True)
        df_all = pd.concat([df_all, df_result]).reset_index(drop=True)
    # 统一省份名
    df_all = pd.merge(df_all, df_city, left_on='province', right_on='全称')[['区域', '拼音', 'date', 'value']]
    df_all = df_all.groupby(['区域', '拼音', 'date']).mean().reset_index()
    # 输出
    df_all.to_csv(os.path.join(raw_path, 'gdp_raw.csv'), index=False, encoding='utf_8_sig')


def ratio():
    # 按地区算各省份gdp占比
    df_all = pd.read_csv(os.path.join(raw_path, 'gdp_raw.csv'))
    df_sum = df_all.groupby(['区域', 'date']).sum().reset_index().rename(columns={'value': 'sum'})
    df_all = pd.merge(df_all, df_sum)
    df_all['ratio'] = df_all['value'] / df_all['sum']
    df_all = df_all[['区域', '拼音', 'date', 'ratio']].reset_index(drop=True)
    df_all['date'] = pd.to_datetime(df_all['date'])
    # 计算各省份指标占比
    df = pd.read_csv(os.path.join(file_path, '各地区旅客吞吐量.csv'))
    df['date'] = pd.to_datetime(df['date'], format='%Y年%m月')
    df_ratio = pd.merge(df, df_all, left_on=['date', 'region'], right_on=['date', '区域'])
    df_ratio['value'] = df_ratio['zhibiao'] * df_ratio['ratio']
    # 做最后一步全国占比
    df_ratio = df_ratio[['date', '拼音', 'value']]
    df_country = df_ratio.groupby(['date']).sum().reset_index().rename(columns={'value': 'sum'})
    df_result = pd.merge(df_ratio, df_country)
    df_result['ratio'] = df_result['value'] / df_result['sum']
    df_result = df_result[['date', '拼音', 'ratio']]
    # 缺失值按照前一年的填补
    df_nan = df_result[df_result.isna().any(axis=1)].reset_index(drop=True).drop(columns=['ratio'])
    df_nan['year'] = df_nan['date'].dt.year - 1
    df_nan['month'] = df_nan['date'].dt.month
    df_nan['per_date'] = pd.to_datetime(df_nan[['year', 'month']].assign(Day=1))  # 合并年月
    df_nan = pd.merge(df_nan, df_result, left_on=['per_date', '拼音'], right_on=['date', '拼音'])[
        ['date_x', '拼音', 'ratio']].rename(columns={'date_x': 'date'})

    # 合并缺失值
    df_rest = df_result[~df_result.isna().any(axis=1)].reset_index(drop=True)
    df_result = pd.concat([df_rest, df_nan]).reset_index(drop=True)
    # 输出结果
    df_result.to_csv(os.path.join(raw_path, 'ratio.csv'), index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
