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


def get_cookie(url):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

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
        cookie[1]['name'], cookie[1]['value'], cookie[0]['name'], cookie[0]['value']), 'Host': 'data.stats.gov.cn',
               'Referer': 'https://data.stats.gov.cn/easyquery.htm?cn=E0101'}
    wd.quit()
    return headers


def get_json(dv, vc, dr):
    import re
    import time
    import json
    import pandas as pd
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument('--ignore-ssl-errors=yes')  # 这两条会解决页面显示不安全问题
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')

    dbcode_value = dv
    wds_value = '[{"wdcode":"zb","valuecode":"%s"}]' % vc
    dfwds_value = '[{"wdcode":"sj","valuecode": "%s"}]' % dr
    k1_value = str(int(time.time() * 1000))
    url = 'https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=%s&rowcode=reg&colcode=sj&wds=%s&dfwds=%s' \
          '&k1=%s&h=1' % (dbcode_value, wds_value, dfwds_value, k1_value)
    # 开始爬
    wd = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)  # 打开浏览器
    wd.get(url)
    # 得到网页源码
    content = wd.page_source
    # 提取有效数据
    json_data = re.compile(r'pre-wrap;">(?P<data>.*?)</pre>', re.S)
    result = json.loads(json_data.findall(content)[0])
    # 提取数据
    name = []
    date = []
    data = []
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
    wd.quit()
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

    image_path = './image/updated/'

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
