
import requests
import pandas as pd
import os
import datetime
import calendar

# ###############################参数############################################
end_year = int(datetime.datetime.now().strftime('%Y')) + 1  # 当年+1
out_path = '/data/global_data\\workday.csv'
url = 'http://www.fynas.com/workday/count'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/99.0.4844.82 Safari/537.36 ',
           'Cookie': 'Hm_lvt_0fa7b8c467469bd8f2eaddd5dc1d440d=1648287024'}

# #################################################################################
for i in range(1990, end_year):
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
            pd.DataFrame(result).to_csv(out_path, mode='a', header=False, index=False, encoding='utf_8_sig')
