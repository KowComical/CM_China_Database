from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
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

env_path = '/data/xuanrenSong/CM_China_Database'
file_path = os.path.join(env_path, 'data', 'global_data')


def main():
    upload()


def upload():
    wd = webdriver.Chrome(service=Service(ChromeDriverManager().install()))  # 打开浏览器
    wd.implicitly_wait(60)
    wd.get('https://lsce:lsce2021BPwd@datas.carbonmonitor.org/admin/')  # 登录网址

    wd.find_element(By.XPATH, "//a[@data-source='carbon_china']").click()  # 点击carbon_china这一行
    wd.find_element(By.XPATH, "//label[@class='custom-file-upload']").click()  # 点击上传

    time.sleep(20)
    pyautogui.typewrite('/data/xuanrenSong/CM_China_Database/data/global_data/all_data.csv')  # 输入文件

    time.sleep(20)
    pyautogui.press('enter')  # 点击确定
    time.sleep(20)
    wd.find_element(By.XPATH, "//button[@class='enabled']").click()  # 点击确认上传

    time.sleep(300)
    # 点击active之前有时会卡住 所以需要刷新一下网页
    wd.refresh()
    time.sleep(120)
    wd.find_element(By.XPATH, "//div[@class='active_radiobt']").click()  # 点击active
    time.sleep(60)
    wd.quit()  # 关闭


if __name__ == '__main__':
    main()
