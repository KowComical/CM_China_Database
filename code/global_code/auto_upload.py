import requests
import pandas as pd
import os

BASE_URL = '####'
CREDENTIALS = '####'

sector_dict = {
    '主站': 'carbon_global',
    '美国': 'carbon_us',
    '中国': 'carbon_china',
    '城市': 'carbon_cities',
    '电力': 'energy_global',
    '欧洲': 'carbon_eu'
}


def main():
    file_path = '/data3/kow/CM_China_Database/data/global_data/cm_china.csv'
    sector_type = '中国'
    # 上传数据
    upload_file(file_path, sector_type)
    # 找到新数据的ID
    file_id = find_file_active_ID(sector_type)
    # 激活新数据
    active_file(sector_type, file_id)
    # 删除所有其他数据
    remove_file(sector_type)

    print(f"{sector_type}部门的新数据 - {os.path.basename(file_path)} - 已经成功上传了")


def upload_file(file_path, sector_type):
    with open(file_path, 'rb') as file:
        files = {'fileToUpload': file}
        data = {'source': sector_dict[sector_type]}

        upload_url = f"https://{CREDENTIALS}@{BASE_URL}insertCSVTableFile.php"
        response = requests.post(upload_url, files=files, data=data)

        if response.status_code != 200:
            print("上传数据失败.")
            raise


def find_file_active_ID(sector_type):
    html = requests.get(f"https://{CREDENTIALS}@{BASE_URL}getExtractions.php")

    if html.status_code != 200:
        print("找到ID失败.")
        raise

    df = pd.DataFrame(html.json()['extractions'])
    df = df[df['source'] == sector_dict[sector_type]].reset_index(drop=True)

    if df.iloc[0]['active'] == '0':
        return df.iloc[0]['id']
    else:
        print('数据是否并未上传成功？')
        raise


def active_file(sector_type, file_id):
    activation_url = f"https://{CREDENTIALS}@{BASE_URL}activateExtraction.php"
    activation_data = {'source': sector_dict[sector_type], 'activeID': file_id}
    activation_response = requests.post(activation_url, data=activation_data)

    if activation_response.status_code != 200:
        print("激活新数据失败.")
        raise


def remove_file(sector_type):
    html = requests.get(f"https://{CREDENTIALS}@{BASE_URL}getExtractions.php")

    if html.status_code != 200:
        print("找到ID失败.")
        raise

    df = pd.DataFrame(html.json()['extractions'])
    df = df[df['source'] == sector_dict[sector_type]]
    df = df[df['active'] != '1'].reset_index(drop=True)

    for file_id in df['id'].unique()[3:]:  # 保留最近3份历史数据
        remove_url = f"https://{CREDENTIALS}@{BASE_URL}deleteExtraction.php"
        remove_data = {'source': sector_dict[sector_type], 'extractionID': file_id}
        remove_response = requests.post(remove_url, data=remove_data)

        if remove_response.status_code != 200:
            print("删除文件失败.")
            raise


if __name__ == '__main__':
    main()
