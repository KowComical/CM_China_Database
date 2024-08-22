from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import sys
import pandas as pd

sys.dont_write_bytecode = True
env_path = '/data/xuanrenSong/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af

file_path = os.path.join(env_path, 'data', 'global_data')
download_path = '/home/xuanrenSong'


def main():
    craw()


def craw():
    # 开始下载新的 这里有一个bug 如果命名不是carbonmonitor 则会出错 可以单独新建个文件夹其实 但目前暂时先不用 除了bug再说
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
    options.add_argument('--ignore-certificate-errors')
    # prefs = {"download.default_directory": file_path}
    # options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)  # 打开浏览器
    driver.implicitly_wait(60)
    driver.get('https://lsce:lsce2021BPwd@datas.carbonmonitor.org/admin/')  # 登录网址
    time.sleep(10)
    driver.find_element(By.XPATH, "//a[@class='row_downloadbt']").click()  # 下载最新的CM_Emission data
    time.sleep(5)
    # 将文件转移位置
    cm_file = af.search_file(download_path)
    cm_file = [cm_file[i] for i, x in enumerate(cm_file) if x.find('carbonmonitor') != -1][0]
    df_cm = pd.read_csv(cm_file)
    df_cm.to_csv(os.path.join(file_path, 'cm.csv'), index=False, encoding='utf_8_sig')
    # 删除不需要的
    os.remove(cm_file)


if __name__ == '__main__':
    main()
