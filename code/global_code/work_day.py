import requests
import pandas as pd
import os
from datetime import datetime
import calendar
import sys

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af

url = 'http://www.fynas.com/workday/count'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/99.0.4844.82 Safari/537.36 ',
           'Cookie': 'Hm_lvt_0fa7b8c467469bd8f2eaddd5dc1d440d=1648287024'}

file_path = './data/Global Data/'
out_path = os.path.join(file_path, 'workday.csv')
file_name = af.search_file(file_path)
file_name = [file_name[i] for i, x in enumerate(file_name) if x.find('workday') != -1][0]
df = pd.read_csv(file_name)
max_year = max(df['year'])
now_year = int(datetime.now().strftime('%Y'))
if max_year != now_year:
    for i in range(max_year, now_year + 1):
        for t in range(1, 3):
            month_date = calendar.monthrange(i, t)[1]
            params = {'start_date': str(i) + '-0' + str(t) + '-01',
                      'end_date': str(i) + '-0' + str(t) + '-' + str(month_date)}
            r = requests.post(url, data=params, headers=headers)
            result = dict(pd.json_normalize(r.json()['data']))
            result['year'] = i
            result['month'] = t
            if not os.path.exists(out_path):
                pd.DataFrame(result).to_csv(out_path, index=False, encoding='utf_8_sig')
            else:
                pd.DataFrame(result).to_csv(out_path, mode='a', header=False, index=False,
                                            encoding='utf_8_sig')
