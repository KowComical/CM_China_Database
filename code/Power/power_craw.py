# 数据来源 Zhu Deng
import pandas as pd
import os
from selenium import webdriver
import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af

# 参数
file_path = './data/Power/craw/'

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
options.add_argument('--ignore-certificate-errors')


def main():
    craw()


def craw():
    for k in ['A03010H01', 'A03010H02']:
        # 爬取数据
        df_result = af.get_json('fsyd', k, 'LAST13')
        # 输出
        if k == 'A03010H01':
            file = os.path.join(file_path, '火电当月.csv')
        else:
            file = os.path.join(file_path, '火电累计.csv')

        # 删除重复值
        # 读取历史数据
        df_history = pd.read_csv(file)
        # 合并结果并删除重复值
        df_result = pd.concat([df_result, df_history]).reset_index(drop=True)
        df_result = df_result[~df_result.duplicated(['name', 'date'])].reset_index(drop=True)
        df_result.to_csv(file, index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
