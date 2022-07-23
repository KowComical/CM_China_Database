from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import sys

sys.dont_write_bytecode = True
sys.path.append('K:\\Github\\CM_China_Database\\code\\')
from global_code import global_function as af

file_path = 'K:\\Github\\CM_China_Database\\data\\Global Data\\'
# 下载之前先把旧的删掉
file_name = af.search_file(file_path)
file_name = [file_name[i] for i, x in enumerate(file_name) if x.find('carbonmonitor') != -1][0]
del file_name

# 开始下载新的 这里有一个bug 如果命名不是carbonmonitor 则会出错 可以单独新建个文件夹其实 但目前暂时先不用 除了bug再说
options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
options.add_argument('--ignore-certificate-errors')
prefs = {"download.default_directory": file_path}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)  # 打开浏览器
driver.implicitly_wait(60)
driver.get('https://lsce:lsce2021BPwd@datas.carbonmonitor.org/admin/')  # 登录网址
time.sleep(10)
driver.find_element(By.XPATH, "//a[@class='row_downloadbt']").click()  # 下载最新的CM_Emission data
time.sleep(5)
