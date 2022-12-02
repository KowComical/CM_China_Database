import re
import time

import numpy as np
import requests
import pandas as pd
import os

headers = {
    'User-Agent': 'Mozilla'
                  '/5.0 (Macintosh; Intel Mac OS X 10_14) ''AppleWebKit'
                  '/605.1.15 (KHTML, like Gecko) ''Version/12.0 Safari/605.1.15'}


def search_file(file_path):
    import os
    import sys
    sys.dont_write_bytecode = True
    file_name = []
    for parent, surnames, filenames in os.walk(file_path):
        for fn in filenames:
            file_name.append(os.path.join(parent, fn))
    return file_name


file_path = './code/test/data/'
kow = pd.read_csv(os.path.join(file_path, 'kow.csv'))['0'].tolist()

file_name = search_file(file_path)
file_name = [file_name[i] for i, x in enumerate(file_name) if not x.find('kow.csv') != -1]
name = re.compile(r'test/data/(?P<name>.*?).csv', re.S)
all_file = [int(name.findall(f)[0]) for f in file_name]

# city_country = pd.DataFrame()
for i in range(3000, len(kow)):
    if i not in all_file:
        lat = float(kow[i].split(',')[0])
        lon = float(kow[i].split(',')[1])
        if not np.isnan(lat) or not np.isnan(lon):
            url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&accept-language=en&zoom=10'
            result = requests.get(url=url, headers=headers, timeout=60)
            temp = pd.DataFrame([result.json()['address']])
            temp['lat'] = lat
            temp['lon'] = lon
            temp.to_csv(os.path.join(file_path, '%s.csv' % i), index=False, encoding='utf_8_sig')
            time.sleep(2)
    # city_country = pd.concat([city_country,temp]).reset_index(drop=True)
