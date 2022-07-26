from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import logging

logging.getLogger('WDM').setLevel(logging.NOTSET)  # 关闭运行chrome时的打印内容

import time
import pandas as pd
import os
import sys

sys.dont_write_bytecode = True
import pyautogui

import logging

logging.getLogger('WDM').setLevel(logging.NOTSET)  # 关闭运行chrome时的打印内容

file_path = './data/Global Data/'


def main():
    upload()


def upload():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))  # 打开浏览器
    driver.implicitly_wait(60)
    driver.get('https://lsce:lsce2021BPwd@datas.carbonmonitor.org/admin/')  # 登录网址

    driver.find_element(By.XPATH, "//a[@data-source='carbon_china']").click()  # 点击energy global这一行
    driver.find_element(By.XPATH, "//label[@class='custom-file-upload']").click()  # 点击上传
    time.sleep(5)
    # 试试这样
    df = pd.read_csv(os.path.join(file_path, 'all_data.csv'))
    df.to_csv('C:\\all_data.csv', index=False, encoding='utf_8_sig')
    time.sleep(5)
    pyautogui.write('C:\%s.csv' % 'all_data')  # 输入文件
    time.sleep(1)
    pyautogui.press('enter')  # 点击确定

    driver.find_element(By.XPATH, "//button[@class='enabled']").click()  # 点击确认上传
    time.sleep(10)
    # 点击active之前有时会卡住 所以需要刷新一下网页
    driver.refresh()
    time.sleep(5)
    driver.find_element(By.XPATH, "//div[@class='active_radiobt']").click()  # 点击active
    time.sleep(5)
    driver.close()  # 关闭


if __name__ == '__main__':
    main()
