def search_file(file_path):
    import os
    import sys
    sys.dont_write_bytecode = True
    file_name = []
    for parent, surnames, filenames in os.walk(file_path):
        for fn in filenames:
            file_name.append(os.path.join(parent, fn))
    return file_name


def read_daily(sector):
    import pandas as pd
    import os
    df = pd.read_csv('/data3/kow/CM_Database/data/global/latest/CM_website.csv')  # 全国日排放

    df = df[(df['country'] == 'China') & (df['sector'].str.contains(sector))].reset_index(drop=True)
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    df = df.groupby(['date']).sum(numeric_only=True).reset_index().drop(columns=['timestamp'])

    return df


def out_put(df, out_path, sector):
    import os

    df['sector'] = sector

    df.to_csv(os.path.join(out_path, f'{sector}_result.csv'), index=False, encoding='utf_8_sig')


def get_cookie(url):
    driver = setup_webdriver()
    try:
        driver.get(url)
        cookies = driver.get_cookies()
        # 尝试下chatgpt推荐的写法
        cookie_string = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/104.0.0.0 Safari/537.36',
            'Cookie': cookie_string,
            'Host': 'data.stats.gov.cn',
            'Referer': 'https://data.stats.gov.cn/easyquery.htm?cn=E0101'
        }

        return headers

    finally:
        driver.quit()


# 不知道为啥 但是rowcode这个字段很重要
# 现在是rowcode=reg
# 有些情况要等于zb
def get_json(dv, vc, dr, jing):
    import re
    import time
    import json
    import pandas as pd
    import requests
    import urllib3
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    dbcode_value = dv
    wds_value = '[{"wdcode":"zb","valuecode":"%s"}]' % vc
    dfwds_value = '[{"wdcode":"sj","valuecode":"%s"}]' % dr
    k1_value = str(int(time.time() * 1000))
    url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=%s&rowcode=reg&colcode=sj&wds=%s&dfwds=%s' \
          '&k1=%s&h=1' % (dbcode_value, wds_value, dfwds_value, k1_value)

    # selenium办法有乱码 改用requests 但是要加sleep-time

    time.sleep(5)
    html = requests.get(url, verify=False)

    result = html.json()

    # # selenium 办法
    # wd = setup_webdriver()
    # try:
    #     wd.get(url)
    #     # 得到网页源码
    #     content = wd.page_source
    #     # 提取有效数据
    #     json_data = re.compile(r'pre-wrap;">(?P<data>.*?)</pre>', re.S)
    #     extracted_content = json_data.findall(content)[0]
    #     result = json.loads(extracted_content)
    #
    # finally:
    #     wd.quit()

    # 提取数据
    name = []
    date = []
    data = []
    if jing:
        city_list = pd.json_normalize(result['returndata']['wdnodes'][0], record_path='nodes')['name'].tolist()
        time_list = pd.json_normalize(result['returndata']['wdnodes'][1], record_path='nodes')['name'].tolist()
    else:
        city_list = pd.json_normalize(result['returndata']['wdnodes'][1], record_path='nodes')['name'].tolist()
        time_list = pd.json_normalize(result['returndata']['wdnodes'][2], record_path='nodes')['name'].tolist()
    for c in city_list:
        for t in time_list:
            name.append(c)
            date.append(t)
    data_list = result['returndata']['datanodes']
    for i in range(len(data_list)):
        data.append(pd.DataFrame([data_list[i]['data']])['data'].tolist()[0])
    df_result = pd.concat([pd.DataFrame(name, columns=['name']), pd.DataFrame(date, columns=['date']),
                           pd.DataFrame(data, columns=['data'])], axis=1)

    return df_result


def get_size(txt, font):
    from PIL import Image
    from PIL import ImageDraw

    testImg = Image.new('RGB', (1, 1))
    testDraw = ImageDraw.Draw(testImg)
    return testDraw.textsize(txt, font)


def update_date(df):
    import os
    from PIL import Image
    from PIL import ImageDraw
    from PIL import ImageFont

    env_path = '/data3/kow/CM_China_Database'
    image_path = os.path.join(env_path, 'image', 'updated')

    # 参数
    fontname = os.path.join(image_path, 'PingFang-Jian-ChangGuiTi-2.ttf')
    fontsize = 20
    colorText = "black"
    colorBackground = "white"
    font = ImageFont.truetype(fontname, fontsize)
    # 找到各部门的最大更新日期
    sector_list = df['sector'].unique()
    for s in sector_list:
        max_date = df[df['sector'] == s]['date'].max().strftime('%Y-%m-%d')
        width, height = get_size(max_date, font)
        img = Image.new('RGB', (width + 20, height + 20), colorBackground)
        d = ImageDraw.Draw(img)
        d.text((10, height / 4), max_date, fill=colorText, font=font)
        img.save(os.path.join(image_path, '%s.png' % s))


def create_folder(file_path, sector_type):  # 建立需要的文件夹
    import os
    from pathlib import Path
    out_path = os.path.join(file_path, sector_type)
    Path(out_path).mkdir(parents=True, exist_ok=True)
    return out_path


def useful_element(sector):
    import os
    from pathlib import Path
    env_path = '/data3/kow/CM_China_Database'

    global_path = Path(env_path, 'data')
    raw_path = os.path.join(global_path, sector, 'raw')
    craw_path = os.path.join(global_path, sector, 'craw')
    useful_path = os.path.join(global_path, 'global_data')
    out_path = os.path.join(global_path, sector, 'cleaned')

    return global_path, raw_path, craw_path, useful_path, out_path


def setup_webdriver(download_path=False):
    from selenium import webdriver
    import undetected_chromedriver as uc

    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
    chrome_options.add_argument('--ignore-certificate-errors')

    prefs = {
        "download.default_directory": download_path,
        "download.directory_upgrade": True,
        "download.prompt_for_download": False,
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "excludeSwitches": ["enable-logging"]
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = uc.Chrome(options=chrome_options, version_main=119)

    driver.implicitly_wait(60)

    return driver


def process_workday():
    import pandas as pd

    work = pd.read_csv('/data3/kow/CM_China_Database/data/global_data/workday.csv')
    work['date'] = work['年份'].astype(str) + '年' + work['月份'].astype(str) + '月'
    # 计算workday占比
    work = work[work['月份'].isin([1, 2])].reset_index(drop=True)  # 只需要1 2月
    year_list = work['年份'].unique()
    ratio_list = []

    for year in year_list:
        temp = work[work['年份'] == year].reset_index(drop=True)
        jan_workdays = temp['工作日'].tolist()[0]
        feb_workdays = temp['工作日'].tolist()[1]
        total_workdays = jan_workdays + feb_workdays

        jan_ratio = jan_workdays / total_workdays
        feb_ratio = feb_workdays / total_workdays
        ratio_list.append(jan_ratio)
        ratio_list.append(feb_ratio)
    work['ratio'] = ratio_list

    return work


def find_missing_month(df_data):
    # 查找所有列名以确定是否存在“1月”
    columns_to_check = [col for col in df_data.columns if col.endswith('1月')]

    for col in columns_to_check:
        year = col[:-2]  # 获取年份
        next_month_col = f'{year}2月'  # 构造“2月”列名

        # 检查数据框是否包含对应的“2月”列
        if next_month_col not in df_data.columns:
            # 如果不存在对应的“2月”，则删除“1月”的列
            df_data.drop(columns=[col], inplace=True)
    return df_data
