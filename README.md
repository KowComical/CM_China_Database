[![all_process](https://github.com/KowComical/CM_China_Database/actions/workflows/all_process.yml/badge.svg)](https://github.com/KowComical/CM_China_Database/actions/workflows/all_process.yml)
[![push_to_database](https://github.com/KowComical/CM_China_Database/actions/workflows/push_to_others.yml/badge.svg)](https://github.com/KowComical/CM_China_Database/actions/workflows/push_to_others.yml)

#### 目前更新是一个季度的延迟

|Sector|Lastest_Date|Description|
|:-:|:-:|:-:|
|**[Aviation](./data/Aviation/)**|![](./image/updated/Aviation.png)|这个部门因为GDP是季度更新 所以最慢 一个季度延迟|
|**[Ground Transport](./data/Ground_Transport/)**|![](./image/updated/Ground_Transport.png)|汽车保有量更新的慢（应该是一年延迟）缺失的年份ratio用线性推|
|**[Industry](./data/Industry/)**|![](./image/updated/Industry.png)|类型ratio并不知道如何得到的 并且我记得之前提过一次 类型并不全<br>两个月延迟|
|**[Power](./data/Power/)**|![](./image/updated/Power.png)|排放因子按照之前推的2021和2022年的|
|**[Residential](./data/Residential/)**|![](./image/updated/Residential.png)|供热面积从2021年开始没有数据 用的线性推的 效果不错|
