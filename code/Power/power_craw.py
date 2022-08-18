# 数据来源 Zhu Deng
import os
from datetime import datetime
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
    # 设置爬取范围
    end_year = datetime.now().strftime('%Y')
    date_range = '2019-%s' % end_year
    for k in ['A03010H01', 'A03010H02']:
        # 爬取数据
        df_result = af.get_json('fsyd', k, date_range)
        # 输出
        if k == 'A03010H01':
            df_result.to_csv(os.path.join(file_path, '火电当月.csv'), index=False, encoding='utf_8_sig')
        else:
            df_result.to_csv(os.path.join(file_path, '火电累计.csv'), index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
