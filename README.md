[![all_process](https://github.com/KowComical/CM_China_Database/actions/workflows/all_process.yml/badge.svg)](https://github.com/KowComical/CM_China_Database/actions/workflows/all_process.yml)

|Sector|Lastest_Date|Description|
|:-:|:-:|:-:|
|**[Aviation](./data/Aviation/)**|![](./image/updated/Aviation.png)|ratio 按照各省GDP比例拆分到省的 GDP数据会延后一点 缺失的会按照前一年的比例填充|
|**[Ground Transport](./data/Ground_Transport/)**|![](./image/updated/Ground_Transport.png)|汽车保有量更新的慢（应该是一年延迟）缺失的年份ratio用线性推|
|**[Industry](./data/Industry/)**|![](./image/updated/Industry.png)|类型ratio并不知道如何得到的 并且我记得之前提过一次 类型并不全|
|**[Power](./data/Power/)**|![](./image/updated/Power.png)|排放因子按照之前推的2021和2022年的|
|**[Residential](./data/Residential/)**|![](./image/updated/Residential.png)|供热面积从2021年开始没有数据 用的线性推的 效果不错|
