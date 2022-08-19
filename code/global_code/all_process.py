import warnings

warnings.filterwarnings('ignore')
import sys

sys.dont_write_bytecode = True

try:
    sys.path.append('./code/global_code/')
    print('#######################')
    print('Begin process Download Daily...')
    import download_daily as dd

    dd.main()
    print('Finish process Download Daily...')
except Exception as e:
    print(e)

try:
    sys.path.append('./code/Aviation/')
    print('#######################')
    print('Begin process Aviation...')
    import aviation as av

    av.main()
    print('Finish process Aviation...')
except Exception as e:
    print(e)

try:
    sys.path.append('./code/Ground_Transport/')
    print('#######################')
    print('Begin process Ground Transposrt...')
    import ground_transport as gt

    gt.main()
    print('Finish process Ground Transposrt...')
except Exception as e:
    print(e)

try:
    sys.path.append('./code/Industry/')
    print('#######################')
    print('Begin process Industry...')
    import industry as i

    i.main()
    print('Finish process Industry...')
except Exception as e:
    print(e)

try:
    sys.path.append('./code/Power/')
    print('#######################')
    print('Begin process Power...')
    import power as p

    p.main()
    print('Finish process Power...')
except Exception as e:
    print(e)

try:
    sys.path.append('./code/Residential/')
    print('#######################')
    print('Begin process Residential...')
    import residential as re

    re.main()
    print('Finish process Residential...')
except Exception as e:
    print(e)

try:
    sys.path.append('./code/global_code/')
    print('#######################')
    print('Begin process All Sum...')
    import all_sum as alls

    alls.main()
    print('Finish process All Sum...')
except Exception as e:
    print(e)
