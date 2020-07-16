# my2sql
go版MySQL binlog解析工具，通过解析MySQL binlog ，可以生成原始SQL、回滚SQL、去除主键的INSERT SQL等，也可以生成DML统计信息。类似工具有binlog2sql、MyFlash、my2fback等，本工具基于my2fback、binlog_inspector工具二次开发而来。


# 用途
* 数据快速回滚(闪回)
* 主从切换后新master丢数据的修复
* 从binlog生成标准SQL，带来的衍生功能
* 生成DML统计信息，可以找到哪些表更新的比较频繁
* IO高TPS高， 查出哪些表在频繁更新
* 找出某个时间点数据库是否有大事务或者长事务
* 除了支持常规数据类型，对大部分工具不支持的数据类型做了支持，比如json、blob、text、emoji等数据类型sql生成


# 产品性能对比
binlog2sql当前是业界使用最广泛的工具，下面对my2sql和binlog2sql做个性能对比。

|                          |my2sql     |binlog2sql|
|---                       |---         |---   |
|1.1G binlog生成回滚SQL      |  1分40秒   |    65分钟  |
|1.1G binlog生成原始SQL      |  1分30秒   |     50分钟|
|1.1G binlog生成表DML统计信息      |   40秒     |不支持|


# 使用案例
### 解析出标准SQL
#### 根据时间点解析出标准SQL
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -work-type 2sql  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir

#### 根据pos点解析出标准SQL
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -work-type 2sql  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir


### 解析出回滚SQL
#### 根据时间点解析出回滚SQL
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -work-type rollback  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir

#### 根据pos点解析出回滚SQL
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -work-type rollback  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir


### 统计DML以及大事务
#### 统计时间范围各个表的DML操作数量，统计一个事务大于500条、时间大于300秒的事务
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -work-type stats  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00"  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir

#### 统计一段pos点范围各个表的DML操作数量，统计一个事务大于500条、时间大于300秒的事务
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -work-type stats  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir








