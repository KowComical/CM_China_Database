def search_file(file_path):
    import os
    import sys
    sys.dont_write_bytecode = True
    file_name = []
    for parent, surnames, filenames in os.walk(file_path):
        for fn in filenames:
            file_name.append(os.path.join(parent, fn))
    return file_name


def read_daily(useful_path, sector):
    import pandas as pd
    cm_path = search_file(useful_path)
    cm_path = [cm_path[i] for i, x in enumerate(cm_path) if x.find('carbonmonitor') != -1][0]
    df = pd.read_csv(cm_path)  # 全国日排放
    df = df[(df['country'] == 'China') & (
        df['sector'].str.contains(sector))].reset_index(drop=True)
    df = df.groupby(['date']).sum().reset_index().drop(columns=['timestamp'])
    return df


def out_put(df, out_path, sector):
    from datetime import datetime
    import os
    # 输出输出两个版本
    now_date = datetime.now().strftime('%Y-%m-%d')
    # 第一个带日期放在history文件夹里备用
    df['sector'] = sector.title()
    df.to_csv(os.path.join(out_path, 'history', '%s_result_%s.csv' % (sector, now_date)), index=False,
              encoding='utf_8_sig')
    # 第二个放在外面当最新的用
    df.to_csv(os.path.join(out_path, '%s_result.csv' % sector), index=False, encoding='utf_8_sig')


def get_cookie():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    url = 'https://data.stats.gov.cn/easyquery.htm'
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    wd = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)  # 打开浏览器
    wd.get(url)  # 打开要找cookie的网址
    cookie = wd.get_cookies()  # 获取cookie
    # 将cookie添加到header里
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/104.0.0.0 Safari/537.36', 'Cookie': '%s=%s; %s=%s' % (
        cookie[1]['name'], cookie[1]['value'], cookie[0]['name'], cookie[0]['value'])}
    wd.quit()
    return headers
