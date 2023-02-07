import pandas as pd
import os
import numpy as np

from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt

import global_function as af

out_path = './image/'
file_path = './data/global_data/'


def main():
    draw_pic()


def draw_pic():
    df = pd.read_csv(os.path.join(file_path, 'all_data.csv')).drop(columns={'timestamp'})
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    df_sum = df.groupby(['state', 'date']).sum(numeric_only=True).reset_index()
    df_sum['sector'] = 'Total'
    df = pd.concat([df, df_sum]).reset_index(drop=True)

    # 列转行
    df = pd.pivot_table(df, index=['date', 'state'], values='value', columns='sector').reset_index()
    df['year'] = df['date'].dt.year
    df['month_date'] = df['date'].dt.strftime('%m-%d')
    # 开始画图
    # 参数
    sector_list = ['Aviation', 'Total', 'Ground Transport', 'Power', 'Industry', 'Residential']
    palette = sns.color_palette("Set2", (len(sector_list)))

    current_date = datetime.now().strftime('%Y%m%d')
    current_year = int(datetime.now().strftime('%Y'))
    current_month = datetime.now().strftime('%m')
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '01']

    pro_list = df['state'].unique()
    num = [i for i in range(len(pro_list))]

    for category_name, color_choose in zip(sector_list, palette):
        if category_name == 'Total':
            color_choose = 'tab:blue'
        if category_name == 'Ground Transport':
            category_name = 'Ground_Transport'
        out_name = category_name.capitalize()
        fig = plt.figure(figsize=(140, 100))
        for co, i in zip(pro_list, num):
            plt.subplot(7, 5, i + 1)
            test = df[df['state'] == co].reset_index(drop=True)
            test = test[['state', 'year', 'month_date', category_name]]
            df_2022 = test[test['year'] == 2022].reset_index(drop=True)
            df_rest = test[~test['year'].isin([2022])].reset_index(drop=True)

            # 列转行
            min_max = pd.pivot_table(df_rest, index='year', values=category_name,
                                     columns='month_date').reset_index().drop(
                columns=['year'])
            min_list = []
            max_list = []
            date_list = []
            for c in min_max.columns:
                min_list.append(min(min_max[c]))
                max_list.append(max(min_max[c]))
                date_list.append(c)
            df_result = pd.DataFrame()
            df_result['min'] = min_list
            df_result['max'] = max_list
            df_result['month_date'] = date_list
            df_result = df_result[df_result['month_date'] != '02-29'].reset_index(drop=True)
            df_min = df_result[['month_date', 'min']]
            df_max = df_result[['month_date', 'max']]

            ax = df_max.set_index('month_date')['max'].plot(color='tab:brown', linewidth=15, alpha=0,
                                                            label='_nolegend_')
            df_min.set_index('month_date')['min'].plot(ax=ax, color='tab:grey', linewidth=15, alpha=0,
                                                       label='_nolegend_')
            plt.fill_between(df_max['month_date'], df_max['max'], df_min['min'], alpha=0.5, color='tab:grey')
            df_2022.set_index('month_date')[category_name].plot(color=color_choose, linewidth=18)

            # add the custom ticks and labels
            plt.xticks(np.linspace(0, 365, 13), months)
            plt.xticks(size=80)
            plt.xlabel('')
            plt.yticks(size=80)
            # legend_labels = ['2022']
            # plt.legend(labels = legend_labels,loc='best', prop={'size': 120},shadow=True,fancybox=True)
            plt.title('%s' % co, size=130)
            ax.yaxis.set_major_locator(plt.MaxNLocator(5))  # 只有5段y轴
            ax = plt.gca()  # 获取边框
            for axis in ['top', 'bottom', 'left', 'right']:
                ax.spines[axis].set_linewidth(6)  # change width
                ax.spines[axis].set_color('black')  # change color

        fig.text(0.5, 1.05, '%s Emission for all provinces' % category_name, ha='center', va='center',
                 size=180)  # 在总图位置添加个标签
        # lines, labels = fig.axes[-1].get_legend_handles_labels()
        legend = fig.legend(labels=['historical ranges (2019~2021)', '2022'], loc='lower center', prop={'size': 150},
                            ncol=5,
                            shadow=True, fancybox=True, frameon=False, bbox_to_anchor=(0.47, 0.05))  # 总图的legend

        for line in legend.get_lines():
            line.set_linewidth(80)
        fig.tight_layout(pad=7)
        # 输出
        out_path_energy = af.create_folder(out_path, 'all')
        out_path_type = af.create_folder(out_path_energy, category_name)
        out_path_yearly = af.create_folder(out_path_type, str(current_year))
        out_path_monthly = af.create_folder(out_path_yearly, '%2.2i' % int(current_month))

        plt.savefig(os.path.join(out_path_monthly, '%s_generation_for_all_country_%s.svg' % (out_name, current_date)),
                    format='svg', bbox_inches='tight')
        plt.savefig(os.path.join(out_path, '%s_generation_for_all_country.svg' % out_name), format='svg',
                    bbox_inches='tight')
        # readme也需要一个
        plt.savefig(os.path.join(out_path, '%s_generation_for_all_country.svg' % out_name), format='svg',
                    bbox_inches='tight')


if __name__ == '__main__':
    main()
