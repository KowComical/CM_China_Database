import pandas as pd
import os
import sys
import time
from datetime import datetime, timezone

sys.dont_write_bytecode = True
sys.path.append('./code/')
from global_code import global_function as af

global_path = './data/'
out_path = os.path.join(global_path, 'Global Data')


def main():
    process()


def process():
    # 定位所有部门的最新数据
    file_name = af.search_file(global_path)
    file_name = [file_name[i] for i, x in enumerate(file_name) if x.find('cleaned') != -1]  # 只要整理完的
    file_name = [file_name[i] for i, x in enumerate(file_name) if not x.find('history') != -1]  # 只要最新的

    df_all = pd.concat([pd.read_csv(f) for f in file_name]).reset_index(drop=True)
    df_all['date'] = pd.to_datetime(df_all['date'])
    time_stamp = []  # 将当地时间转换为时间戳
    for d in df_all['date'].tolist():
        time_stamp.append(time.mktime(d.timetuple()))
    df_all['timestamp'] = time_stamp

    utc_time = []  # 将当地时间时间戳转换为utc时间
    for t in df_all['timestamp'].tolist():
        utc_time.append(datetime.fromtimestamp(t, tz=timezone.utc))
    df_all['utc'] = utc_time

    time_stamp = []  # 将utc时间转换为utc时间戳
    for d in df_all['utc'].tolist():
        time_stamp.append(time.mktime(d.timetuple()))
    df_all['timestamp'] = time_stamp
    df_all['timestamp'] = df_all['timestamp'].astype(int)

    df_all = df_all.drop(columns=['utc'])
    df_all['date'] = df_all['date'].dt.strftime('%d/%m/%Y')
    df_all.to_csv(os.path.join(out_path, 'all_data.csv'), index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
