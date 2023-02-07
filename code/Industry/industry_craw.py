# 数据来源 Zhu Deng

import pandas as pd
import os
import time
import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af

from selenium import webdriver

# 参数
file_path = './data/Industry/craw/'
code_path = os.path.join(file_path, 'code.csv')
out_path = os.path.join(file_path, 'Industry_raw.csv')

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
options.add_argument('--ignore-certificate-errors')


def main():
    craw()


def craw():
    # 读取工业各部门代码名称
    code = pd.read_csv(code_path)
    # 读取原始数据
    df_history = pd.read_csv(out_path)

    code_list = code['code'].tolist()
    name_list = code['cname'].tolist()

    for j, k in zip(code_list, name_list):
        time.slee(2)
        df_result = af.get_json('fsyd', j, 'LAST13')
        df_result['type'] = k
        # 将结果储存到历史数据里
        df_history = pd.concat([df_result, df_history]).reset_index(drop=True)

    # 输出结果
    df_history = df_history[~df_history.duplicated(['name', 'date', 'type'])].reset_index(drop=True)
    df_history.to_csv(out_path, index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
