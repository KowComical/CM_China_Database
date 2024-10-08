# 数据来源 Zhu Deng
import pandas as pd
import os
import sys

sys.dont_write_bytecode = True
env_path = '/data3/kow/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af

# 参数
file_path = os.path.join(env_path, 'data', 'Power', 'craw')


def main():
    craw()


def craw():
    for k in ['A03010H01', 'A03010H02']:
        # 爬取数据
        df_result = af.get_json('fsyd', k, 'LAST13', jing=False)
        # 输出
        if k == 'A03010H01':
            file = os.path.join(file_path, '火电当月.csv')
        else:
            file = os.path.join(file_path, '火电累计.csv')

        # 删除重复值
        # 读取历史数据
        df_history = pd.read_csv(file)
        # 去除乱码
        df_history = df_history[df_history['name'].str.contains('市|省|区')].reset_index(drop=True)
        # 合并结果并删除重复值
        df_result = pd.concat([df_result, df_history]).reset_index(drop=True)
        df_result = df_result[~df_result.duplicated(['name', 'date'])].reset_index(drop=True)
        df_result.to_csv(file, index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    main()
