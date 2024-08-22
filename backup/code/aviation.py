import pandas as pd
import os
import numpy as np
from pathlib import Path

import sys

env_path = '/data/kow/CM_China_Database'
sys.path.append(os.path.join(env_path, 'code'))

from global_code import global_function as af

# 路径
global_path, raw_path, craw_path, useful_path, out_path = af.useful_element('Aviation')
day_out = os.path.join(out_path, 'daily')
sum_out = os.path.join(out_path, 'summed')


# all_out = os.path.join(file_path, 'simulated_just_data')


def main():
    # 目前代码少一步根据不同国家的时差来统一时间
    # pre_process()  # 数据预处理
    # estimate_emission()  # 估算碳排放
    # split_into_coor()  # 将每日碳排放汇总并按照坐标分类（包含国家 城市 和机场）
    process_china()


def pre_process():
    orig_path = os.path.join(craw_path, 'orig')
    all_file = af.search_file(orig_path)
    for a in all_file:
        temp = pd.read_csv(a, on_bad_lines='skip')
        # 整理时间
        temp['TimeSeries'] = pd.to_datetime(temp['TimeSeries']).dt.strftime('%Y-%m-%d')
        # 输出
        date_list = temp['TimeSeries'].unique()
        for d in date_list:
            year = pd.to_datetime(d).year
            craw_out_path = os.path.join(craw_path, 'daily', str(year))
            Path(craw_out_path).mkdir(parents=True, exist_ok=True)  # 没有则生成
            df_temp = temp[temp['TimeSeries'] == d].reset_index(drop=True)
            df_temp.to_csv(os.path.join(craw_out_path, '%s.csv' % d), index=False, encoding='utf_8_sig')


def estimate_emission():
    # 所有需要用到的数据
    file_name = af.search_file(os.path.join(craw_path, 'daily'))  # 所有源数据
    file_name = sorted(file_name)
    df_fuel = pd.read_csv(os.path.join(raw_path, 'all_aircraft.csv'))  # 机型油耗数据
    df_location = pd.read_csv(os.path.join(raw_path, '中国中文城市.csv'))
    # 参数
    # needed_mode = ['G', 'J', 'Q', 'S']  # 只有这些service是客运相关的 不包含货运
    needed_mode = ['J']
    # df_result = pd.DataFrame()

    # 开始循环
    date_all = []
    for f in file_name:
        temp = pd.read_csv(f)
        # 只选取一般航空服务
        temp = temp[temp['Service'].isin(needed_mode)].reset_index(drop=True)
        # 清理数据 去掉什么船啊 火车啊 汽车 直升机的数据
        temp = temp[~temp['ArrAirportName'].str.contains('Station')].reset_index(drop=True)
        temp = temp[~temp['DepAirportName'].str.contains('Station')].reset_index(drop=True)
        temp = temp[~temp['GeneralAcftName'].str.contains('Aircraft')].reset_index(drop=True)
        temp = temp[~temp['GeneralAcftName'].str.contains('Jet')].reset_index(drop=True)
        temp = temp[~temp['GeneralAcftName'].str.contains('Boat')].reset_index(drop=True)
        temp = temp[~temp['GeneralAcftName'].str.contains('Helicopters')].reset_index(drop=True)
        temp = temp[temp['GeneralAcftName'] != 'Bus'].reset_index(drop=True)
        temp = temp[temp['GeneralAcftName'] != 'Train'].reset_index(drop=True)
        # 筛选列名
        temp = temp[['DepAirport', 'ArrAirport', 'FlyingTime', 'GeneralAcft',
                     'GeneralAcftName', 'DistKM', 'TimeSeries', 'LocalDepTime', 'LocalArrTime']]

        df_location = df_location[['iata', 'city', 'state', 'country', 'lat', 'lon', 'airport_name']]
        # 出发
        temp = pd.merge(temp, df_location, left_on='DepAirport', right_on='iata', how='left')
        temp = temp.rename(
            columns={'lat': 'dep_lat', 'lon': 'dep_lon'}).drop(columns=['iata'])
        temp = temp.rename(
            columns={'country': 'dep_country', 'state': 'dep_state', 'city': 'dep_city', 'airport_name': 'dep_airport'})
        # 到达
        temp = pd.merge(temp, df_location, left_on='ArrAirport', right_on='iata', how='left')
        temp = temp.rename(
            columns={'lat': 'arr_lat', 'lon': 'arr_lon'})
        temp = temp.rename(
            columns={'country': 'arr_country', 'state': 'arr_state', 'city': 'arr_city',
                     'airport_name': 'arr_airport'}).drop(columns=['iata'])

        # 总修正列名
        temp = temp.rename(columns={'DepAirport': 'dep_iata', 'ArrAirport': 'arr_iata'})

        # 估算碳排放
        # 飞行时间为0的是未按照计划飞行的
        temp = temp[temp['FlyingTime'] != 0].reset_index(drop=True)
        temp['FlyingTime'] = temp['FlyingTime'] / 60  # 分钟变小时
        temp = pd.merge(temp, df_fuel)
        # The combustion of 1 kilogram (kg) of jet fuel in an aircraft engine produces 3.181 kg of carbon dioxide (CO2)
        temp['emission_t'] = temp['FlyingTime'].astype(float) * temp['fuel_flow'].astype(
            float) * 3.157 / 1000  # kg to t
        if not temp[temp[['emission_t']].isnull().T.any()].empty:
            print('#####')
            print(temp[temp[['emission_t']].isnull().T.any()]['GeneralAcftName'].unique())
        # 如果始发和终点在一个国家则为国内航空否则是国际
        temp['type'] = np.where(temp['arr_country'].eq(temp['dep_country']), 'domestic', 'international')
        # 修改列名
        temp = temp.rename(columns={'DistKM': 'distance', 'GeneralAcft': 'aircraft', 'GeneralAcftName': 'aircraft_name',
                                    'TimeSeries': 'date', 'FlyingTime': 'flight_time', 'fuel_flow': 'fuel_flow_kg',
                                    'LocalDepTime': 'local_dep_time', 'LocalArrTime': 'local_arr_time'})
        # 输出按日的
        # 输出年份
        year = pd.to_datetime(temp['date']).dt.year.unique()[0]
        # 输出日期
        date = temp['date'].unique()[0]
        date_all.append(date)  # 检查是否有重复最后的时候

        year_path = os.path.join(day_out, str(year))
        Path(year_path).mkdir(parents=True, exist_ok=True)  # 没有则生成
        day_path = os.path.join(year_path, '%s.csv' % date)
        temp.to_csv(day_path, index=False, encoding='utf_8_sig')
        # df_result = pd.concat([df_result, temp]).reset_index(drop=True)
    # # 输出总的
    # Path(all_out).mkdir(parents=True, exist_ok=True)  # 没有则生成
    # df_result.to_csv(os.path.join(all_out, 'result.csv'), index=False, encoding='utf_8_sig')


def split_into_coor():
    # 暂时先用日尺度的
    all_file = af.search_file(day_out)
    df_sum = pd.DataFrame()
    for a in all_file:
        temp = pd.read_csv(a).fillna('')  # 必须把缺失的state名字填上 否则无法聚合
        # 只保留需要的列 飞机到哪降落 碳排放算哪的 # 具体时间列暂不需要local_arr_time
        temp = temp[['date', 'arr_airport', 'arr_city', 'arr_lat', 'arr_state', 'arr_country', 'arr_lon',
                     'emission_t', 'type']]
        temp['arr_coordinate'] = temp['arr_lat'].astype(str) + ',' + temp['arr_lon'].astype(str)
        temp = temp.drop(columns=['arr_lon', 'arr_lat']).groupby(
            ['date', 'arr_coordinate', 'arr_airport', 'arr_city', 'arr_state', 'arr_country',
             'type']).sum().reset_index()
        df_sum = pd.concat([df_sum, temp]).reset_index(drop=True)

    df_sum.to_csv(os.path.join(sum_out, 'summed.csv'), index=False, encoding='utf_8_sig')


def process_china():
    # 读取所需的数据
    df_c = pd.read_csv(os.path.join(useful_path, 'city_name.csv'))
    # 将全球数据中的中国数据提取出来
    df_new = pd.read_csv(os.path.join(out_path, 'summed.csv'))
    df_new = df_new[(df_new['arr_country'] == '中国')].reset_index(drop=True)
    df_new['date'] = pd.to_datetime(df_new['date'])
    df_new = df_new[['date', 'arr_state', 'emission_t']]
    df_new = df_new[~df_new['arr_state'].isin(['澳门', '香港', '台湾省'])].reset_index(drop=True)
    df_new = df_new.groupby(['date', 'arr_state']).sum(numeric_only=True).reset_index()
    df_new = pd.merge(df_new, df_c, left_on='arr_state', right_on='全称')[['date', '拼音', 'emission_t']]
    df_new = df_new.rename(columns={'emission_t': 'value', '拼音': 'state'})
    # 输出
    df_new = df_new[['date', 'state', 'value']]
    df_new['value'] = df_new['value'] / 1000000  # t to mt

    af.out_put(df_new, out_path, 'Aviation')


if __name__ == '__main__':
    main()
