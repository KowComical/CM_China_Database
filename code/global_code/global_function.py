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
    df = df.groupby(['date']).sum().reset_index()
    return df


def out_put(df, out_path, sector):
    from datetime import datetime
    import os
    # 输出输出两个版本
    now_date = datetime.now().strftime('%Y-%m-%d')
    # 第一个带日期放在history文件夹里备用
    df['sector'] = sector.capitalize()
    df.to_csv(os.path.join(out_path, 'history', '%s_result_%s.csv' % (sector, now_date)), index=False,
              encoding='utf_8_sig')
    # 第二个放在外面当最新的用
    df.to_csv(os.path.join(out_path, '%s_result.csv' % sector), index=False, encoding='utf_8_sig')
