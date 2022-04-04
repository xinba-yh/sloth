# sloth

## 目标
1、client、consumer可以集成spring、springboot常规版本。  
2、提供使用example。  
3、提供基础的benchmark。


## broker
1、单机部署并支持k8s部署  
2、支持异常日志恢复检查  
3、支持3-5W/S+ tps写入  
4、支持新消息broker推送模式
5、支持日志保留时间。
6、通讯协议支持grpc。

## producer
1、支持消息顺序写入  
-oneWay  
-sync  
2、支持每个producer sync模式，1W/S+ tps写入。  

## consumer
1、支持集群模式消费  
2、支持1个partition、1个thread顺序消费模式。    
>2.1、消费异常可以重试配置次数。  
>2.2、消息消费异常会导致当前partition消费短暂不能消费其他消息。

3、支持多个消费者同时消费，支持重平衡partition。 

## todo
broker  
1、支持按照配置时间，log文件滚动清理。  
2、支持主备模式部署，并最终完成类似redis哨兵模式的集群方式可以横向扩展（该方式实现比较简单，由用户指定连接的broker集群）。
consumer
1、消费者可以从指定时间消费。    
2、消费者失败重试配置次数后，放入死信队列。  
3、支持死信队列重新消费。  
4、支持消费组在消费侧按照tag过滤消息消费。  
5、支持广播消费模式。    
