# 数据来源 Zhu Deng
import re
import pandas as pd
import os
import pdfplumber
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import sys

env_path = '/data3/kow/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af

global_path, raw_path, craw_path, useful_path, out_path = af.useful_element('Aviation')
zhibiao_path = os.path.join(craw_path, '生产资料')

gdp_path = '/data3/kow/CM_China_Database/data/Aviation/craw/GDP.csv'


def main():
    craw_zhibiao()  # 爬取航空指标数据
    extract_pdf()  # 将pdf转为数据
    craw_gdp()  # 爬取gdp数据
    gdp_raw()  # 将gdp整理输出到raw
    ratio()  # 计算所需占比

    # 计算Aviation部分
    process_aviation()


def craw_zhibiao():
    base_url = 'https://www.mot.gov.cn/tongjishuju/minhang/index.html'

    # 下载生产资料数据
    response = requests.get(base_url)
    response.encoding = 'utf-8'  # 设置响应的编码为UTF-8
    html_content = response.text

    soup = BeautifulSoup(html_content, 'html.parser')
    pdf_links = soup.find_all('a', href=lambda href: href and ".pdf" in href)
    for link in pdf_links:
        name = link.get('title', 'No title available')
        if '统计公报' not in name:
            pdf_link = 'https://www.mot.gov.cn/tongjishuju/minhang/' + link['href'].replace('./', '')
            # 下载
            year_name = name.split('中国民航')[1].split('年')[0]

            out_folder = os.path.join(zhibiao_path, year_name)
            if not os.path.exists(out_folder):
                os.makedirs(out_folder)
            out_file = os.path.join(out_folder, f'{name}.pdf')
            # 不存在再下载
            if not os.path.exists(out_file):
                response = requests.get(pdf_link)
                with open(out_file, 'wb') as file:
                    file.write(response.content)
                print(f'{name} - 已下载')


def extract_pdf():
    # 转换整理的pdf
    file_name = af.search_file(zhibiao_path)
    name = re.compile(r'生产资料/.*?/中国民航(?P<name>.*?)份主要', re.S)
    df_result = pd.DataFrame()
    for temp_file in file_name:
        pdf = pdfplumber.open(temp_file)
        dfs = pdf.pages[0].extract_tables()
        temp = pd.DataFrame(dfs[0]).fillna(np.nan).replace('', np.nan).replace('\n', '')

        # 以前很多年份都没有 但是貌似网上可以搜到 未来如果需要之前的数据 可以再找
        if '旅客吞吐量' in temp[0].tolist():  # 如果有旅客吞吐量
            # 提取有效信息范围
            start_index = temp[temp[0] == '旅客吞吐量'].index.tolist()[0]
            temp = temp.iloc[start_index:start_index + 5, :3].reset_index(drop=True)
            temp.columns = ['region', 'unit', 'value']
            temp['date'] = name.findall(temp_file)[0]
            df_result = pd.concat([df_result, temp]).reset_index(drop=True)
    # 列转行
    df_result['value'] = df_result['value'].astype(float)
    df_result = pd.pivot_table(df_result, index=['date', 'unit'], values='value', columns='region').reset_index()
    df_result = df_result.rename(columns={'其中：东部地区': '东部地区'}).drop(columns=['旅客吞吐量'])
    # 行转列
    df_result = df_result.set_index(['date', 'unit']).stack().reset_index().rename(
        columns={'level_2': '地区', 0: 'zhibiao'})
    # 输出
    df_result.to_csv(os.path.join(raw_path, '各地区旅客吞吐量.csv'), index=False, encoding='utf_8_sig')


def craw_gdp():
    # 爬取
    df_result = af.get_json('fsjd', 'A010101', 'LAST12', jing=False)
    # 读取历史数据
    df_history = pd.read_csv(gdp_path)
    # 合并结果并删除重复值
    df_result = pd.concat([df_result, df_history]).reset_index(drop=True)
    # 删除重复值
    df_result = df_result[~df_result.duplicated(['name', 'date'])].reset_index(drop=True)

    # df_result
    # 输出结果
    df_result.to_csv(gdp_path, index=False, encoding='utf_8_sig')


def gdp_raw():
    df_city = pd.read_csv(os.path.join(global_path, 'global_data', 'city_name.csv'))
    df_gdp = pd.read_csv(gdp_path)

    # 将所有中文季度转为日期格式
    q_list = ['第一季度', '第二季度', '第三季度', '第四季度']
    num_list = ['01', '04', '07', '10']
    for q, d in zip(q_list, num_list):
        df_gdp['date'] = df_gdp['date'].str.replace('年%s' % q, '-%s' % d)

    # 所有一个季度的值都为0则删掉并打印
    zero_data = df_gdp.groupby('date')['data'].sum(numeric_only=True) == 0
    dates_to_remove = zero_data[zero_data].index

    if not dates_to_remove.empty:
        df_gdp = df_gdp[~df_gdp['date'].isin(dates_to_remove)].reset_index(drop=True)
        print("GDP还未更新的季度", dates_to_remove.tolist())


    # # 填充缺失值
    # df_gdp = df_gdp.sort_values('date').replace(0, np.nan)
    # pro_list = df_gdp['name'].unique()
    # df_replaced = pd.DataFrame()
    # for pro in pro_list:
    #     temp = df_gdp[df_gdp['name'] == pro].reset_index(drop=True)
    #     null = temp[temp.isna().any(axis=1)]
    #     null_index = null.index.tolist()
    #     for n in null_index:
    #         # 找到缺失的年份 并用前两年的相同季度的增幅来填充今年的
    #         null_year = pd.to_datetime(null['date']).dt.year.tolist()[0]
    #         null_month = pd.to_datetime(null['date']).dt.month.tolist()[0]
    #         per_year = temp[temp['date'] == '%s-%2.2i' % (null_year - 1, null_month)]['data'].tolist()[0]
    #         per_per_year = temp[temp['date'] == '%s-%2.2i' % (null_year - 2, null_month)]['data'].tolist()[0]
    #         year_ratio = per_year / per_per_year
    #         null_value = per_year * year_ratio
    #         # 填充
    #         temp.loc[n, 'data'] = null_value
    #     df_replaced = pd.concat([df_replaced, temp]).reset_index(drop=True)
    #
    # df_gdp = df_replaced.copy()

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
    df_all = df_all.groupby(['区域', '拼音', 'date']).mean(numeric_only=True).reset_index()
    # 输出
    df_all.to_csv(os.path.join(raw_path, 'gdp_raw.csv'), index=False, encoding='utf_8_sig')


def ratio():
    # 按地区算各省份gdp占比
    df_all = pd.read_csv(os.path.join(raw_path, 'gdp_raw.csv'))
    df_sum = df_all.groupby(['区域', 'date']).sum(numeric_only=True).reset_index().rename(columns={'value': 'sum'})
    df_all = pd.merge(df_all, df_sum)
    df_all['ratio'] = df_all['value'] / df_all['sum']
    df_all = df_all[['区域', '拼音', 'date', 'ratio']].reset_index(drop=True)
    df_all['date'] = pd.to_datetime(df_all['date'])
    # 计算各省份指标占比
    df = pd.read_csv(os.path.join(raw_path, '各地区旅客吞吐量.csv'))
    df['date'] = pd.to_datetime(df['date'], format='%Y年%m月')
    df_ratio = pd.merge(df, df_all, left_on=['date', 'region'], right_on=['date', '区域'])
    df_ratio['value'] = df_ratio['zhibiao'] * df_ratio['ratio']
    # 做最后一步全国占比
    df_ratio = df_ratio[['date', '拼音', 'value']]
    df_country = df_ratio.groupby(['date']).sum(numeric_only=True).reset_index().rename(columns={'value': 'sum'})
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


def process_aviation():
    # 国内航空和国际航空的daily数据
    df_daily = af.read_daily('Aviation')
    df_daily['year'] = df_daily['date'].dt.year
    df_daily['month'] = df_daily['date'].dt.month

    # 省级航空比例的数据
    df_ratio = pd.read_csv(os.path.join(raw_path, 'ratio.csv'))
    df_ratio['date'] = pd.to_datetime(df_ratio['date'])
    df_ratio['year'] = df_ratio['date'].dt.year
    df_ratio['month'] = df_ratio['date'].dt.month

    # # 乘以ratio

    df_result = pd.merge(df_daily, df_ratio.drop(columns=['date']))
    df_result['value'] = df_result['value'] * df_result['ratio']

    # 输出
    df_result = df_result.rename(columns={'拼音': 'state'})[['date', 'state', 'value']]

    af.out_put(df_result, out_path, 'Aviation')


if __name__ == '__main__':
    main()
