# sloth

## 目标
1、client、consumer可以集成spring   
2、client、consumer可以集成springboot，指定版本

## broker
1、单机部署并支持k8s部署  
2、支持异常日志恢复检查  
3、支持3-5W/S+ tps写入  
4、支持新消息broker推送模式
5、支持日志保留时间。

## producer
1、支持消息顺序写入  
-oneWay  
-sync  
2、支持每个producer sync模式，1W/S+ tps写入。  

## consumer
1、支持集群模式消费  
2、支持1个partition、1个thread顺序消费模式。  
   注意：1、消费异常可以重试配置次数。  
        2、消息消费异常会导致当前partition消费短暂不能消费其他消息。  
3、支持多个消费者同时消费，支持重平衡partition。  
4、支持消费组在消费侧按照tag过滤消息消费。

## todo  
1、消费者可以指定从头消费或时间消费。  
2、支持按照配置时间，log文件滚动清理。  
3、消费者失败重试配置次数后，放入死信队列。  
4、支持死信队列重新消费。