/*
* Copyright 2022 Ververica Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
  */

- 目录结构含义
```txt
debezium : debezium用到的相关类
schema :  mysql schema(表结构)相关代码
source : mysql-cdc source实现代码,包括全量读mysql,分割器,读取器等相关
table : cdc table实现代码主要以table dynamic factory的实现
resrouces : 该目录用与spi方式动态加载table factory,用于sql创建table找到对应的工厂类
```

- 核心设计亮点
    - 切片划分
      - 均匀分布
      - 非均匀分面
    - 全量切片数据读取
      - 快照读取
      - 数据修正
    - 增量切片数据读取
  
- 关键类作用
  - MySqlSourceEnumerator 初始化
  - MySqlSourceReader 初始化
  - MySqlSourceEnumerator 处理分片请求
  - MySqlSourceReader 处理切片分配请求
  - DebeziumReader 数据处理
  - MySqlRecordEmitter 数据下发
  - MySqlSourceReader 汇报切片读取完成事件
  - MySqlSourceEnumerator 分配增量切片
