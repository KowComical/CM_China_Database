import pandas as pd
import os
from datetime import datetime
import calendar

# 参数
env_path = '/data3/kow/CM_China_Database'


def main():
    get_workday()


def get_days_in_month(year, month):
    return calendar.monthrange(year, month)[1]


def get_workday():
    # 当前年
    now_year = int(datetime.now().strftime('%Y'))
    # 读取文件
    file_path = os.path.join(env_path, 'data', 'global_data')
    out_path = os.path.join(file_path, 'workday.csv')
    # 如果文件存在则从文件最大年开始
    if os.path.exists(out_path):
        df_history = pd.read_csv(out_path)
        max_year = max(df_history['年份'])
    else:
        max_year = 1990

    # 开始爬取
    if max_year != now_year:
        df_temp = []
        for year in range(max_year + 1, now_year + 1):
            table_url = f'https://www.fynas.com/workdays/month/{year}'

            df_table = pd.read_html(table_url, header=0)[0]
            df_table['年份'] = year
            df_temp.append(df_table)

        df_all = pd.concat(df_temp).reset_index(drop=True)

        # 得到当月天数
        df_all['当月天数'] = df_all.apply(lambda row: get_days_in_month(row['年份'], row['月份']), axis=1)
        # 调整一下顺序
        df_all = df_all[['当月天数', '周末', '法定假日', '周末调班', '工作日', '年份', '月份']]

        if not os.path.exists(out_path):
            df_all.to_csv(out_path, index=False, encoding='utf_8_sig')
        else:
            df_all.to_csv(out_path, mode='a', header=False, index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
