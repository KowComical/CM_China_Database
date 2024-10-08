import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

env_path = '/data3/kow/CM_China_Database'

out_path = os.path.join(env_path, 'image')
file_path = os.path.join(env_path, 'data', 'global_data')

# 参数
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '01']

color_list = {'Aviation': 'tab:purple',
              'Total': 'tab:blue',
              'Ground Transport': 'tab:green',
              'Power': 'tab:orange',
              'Industry': 'tab:red',
              'Residential': 'tab:brown'}


def main():
    draw_pic()


def draw_pic():
    df = pd.read_csv(os.path.join(file_path, 'cm_china.csv')).drop(columns=['timestamp'])

    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    df_sum = df.groupby(['state', 'date']).sum(numeric_only=True).reset_index()
    df_sum['sector'] = 'Total'
    df = pd.concat([df, df_sum]).reset_index(drop=True)

    # 列转行
    df = pd.pivot_table(df, index=['date', 'state'], values='value', columns='sector').reset_index()

    df['year'] = df['date'].dt.year
    df['month_date'] = df['date'].dt.strftime('%m-%d')

    # 生成最大年和其他年的列表
    max_year = max(df['year'])
    rest_year_list = sorted(df['year'].unique())

    # 开始画图
    pro_list = df['state'].unique()
    num = [i for i in range(len(pro_list))]

    for category_name, color_choose in color_list.items():
        out_name = category_name.capitalize()
        fig = plt.figure(figsize=(140, 100))
        for co, i in zip(pro_list, num):
            plt.subplot(7, 5, i + 1)
            test = df[df['state'] == co].reset_index(drop=True)
            test = test[['state', 'year', 'month_date', category_name]]
            # 最大年显示为线 其余为阴影
            df_current = test[test['year'] == max_year].reset_index(drop=True).fillna(0)
            df_rest = test[test['year'] != max_year].reset_index(drop=True).fillna(0)

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
            df_current.set_index('month_date')[category_name].plot(color=color_choose, linewidth=18)
            # add the custom ticks and labels
            plt.xticks(np.linspace(0, 365, 13), months)
            plt.xticks(size=80)
            plt.xlabel('')
            plt.yticks(size=80)

            plt.title('%s' % co, size=130)
            ax.yaxis.set_major_locator(plt.MaxNLocator(5))  # 只有5段y轴
            ax = plt.gca()  # 获取边框
            for axis in ['top', 'bottom', 'left', 'right']:
                ax.spines[axis].set_linewidth(6)  # change width
                ax.spines[axis].set_color('black')  # change color

        fig.text(0.5, 1.05, f'{category_name} Emission for all provinces (Mt)', ha='center', va='center',
                 size=180)  # 在总图位置添加个标签

        legend = fig.legend(labels=[f'historical ranges ({rest_year_list[0]}~{rest_year_list[-2]})', max_year],
                            loc='lower center', prop={'size': 150},
                            ncol=5,
                            shadow=True, fancybox=True, frameon=False, bbox_to_anchor=(0.47, 0.05))  # 总图的legend

        for line in legend.get_lines():
            line.set_linewidth(80)
        fig.tight_layout(pad=7)

        # 输出
        plt.savefig(os.path.join(out_path, f'{out_name}_emission_for_all_provinces.svg'), format='svg',
                    bbox_inches='tight')


if __name__ == '__main__':
    main()
