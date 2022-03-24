##实现思路
注：producer、consumer与topic数量成正比。
### producer
> 1、尽量简单为主，对于topic无特殊要求也不需要初始化。  
  2、broker会按照topicName自动生成topic，默认：8个partition。  
  3、partition也可以不指定，broker会自动分配。  

### consumer
> 1、consumer对每个需要消费的partition开启一个新的线程持续消费，顺序执行（异常除外,进入系统默认的异常队列）。  
  2、同一个topic，每个partition最多有一个消费者。  

#### 举例说明  
> topic： test-topic  
  partition: 8个  
--1个消费者 进程内会启动8个线程进行消费。  
--2个消费者 每个进程内启动4个线程进行消费。  
--3个消费者 前两个进程各3个，最后一个进程2个消费者。  

#### 实现细节
> consumer
注意：reblance消息处理加锁顺序执行
一、consumer新增
1、groupName（默认:default）、topic -> broker
2、broker onActive  
>   1、分配clientId，记录 topic - consumerGroup - List<clientId> 关系（内存） 
>   2、broker 根据clientId数量，topic partition数量，分配每个clientId应该消费的partition。
>   3、broker notify 已连接client reblance 包含消费的partition。
>   4、broker notify 新连接client clientId、consumer partition。
3、client开启新的消费线程或检查消费线程。
二、consumer离线|断开
> 1、broker UnActive | 无心跳 | 异常断开，删除分配的topic - consumerGroup - List<clientId> 关系（内存）
> 2、broker notify 广播仍然处于活跃状态的client reblance 信息。
> 3、client开启新的消费线程或检查消费线程。
三、补偿
> 1、consumer消息时，如果consumer为集群模式，添加clientId是否可以进行消费的判断，不符合的partitoin返回CONSUMER_PARTITION_NOT_ALLOWED。
> 2、client定时心跳，broker响应需要消费的partition，并检查。
四、consumer提交consumerOffset