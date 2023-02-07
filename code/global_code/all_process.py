from contextlib import contextmanager
import time


@contextmanager
# 设置一个可以监测每个function运行的时间的function
def timeit_context(name):
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time
    print('##### [{}] finished in {:.2f} minutes #####'.format(name, elapsed_time / 60))


import re
import sys

sys.path.append('./code/')
import global_code.download_daily as gd
import Aviation.aviation as aa
import Ground_Transport.ground_transport as gg
import Industry.industry as ii
import Power.power as pp
import Residential.residential as rr
import global_code.all_sum as ga
import global_code.draw_pic as dp


# from contextlib import suppress
func_list = [gd, aa, gg, ii, pp, rr, ga, dp]
for my_func in func_list:
    # with suppress(Exception):  # 如果出错则pass（并不报错） 这里并不用这种办法
    function_name = re.findall(r"([^\\/]*).....$", str(my_func))[0]
    with timeit_context(function_name):
        try:
            my_func.main()
        except Exception as e:
            print(e)
