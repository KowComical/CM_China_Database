# 数据来源 Zhu Deng

import pandas as pd
import os
import time
import sys

env_path = '/data3/kow/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af

# 参数
file_path = os.path.join(env_path, 'data', 'Industry', 'craw')
code_path = os.path.join(file_path, 'code.csv')
out_path = os.path.join(file_path, 'industry_raw.csv')


def main():
    craw()


def craw():
    # 读取工业各部门代码名称
    code = pd.read_csv(code_path)
    # 读取原始数据
    df_history = pd.read_csv(out_path)
    # 去除乱码
    df_history = df_history[df_history['name'].str.contains('市|省|区')].reset_index(drop=True)

    code_list = code['code'].tolist()
    name_list = code['cname'].tolist()

    for j, k in zip(code_list, name_list):
        time.sleep(5)
        df_result = af.get_json('fsyd', j, 'LAST13', jing=False)

        df_result['type'] = k
        # 将结果储存到历史数据里
        df_history = pd.concat([df_result, df_history]).reset_index(drop=True)

    # 输出结果
    df_history = df_history[~df_history.duplicated(['name', 'date', 'type'])].reset_index(drop=True)
    df_history.to_csv(out_path, index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
