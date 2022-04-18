package com.tsingj.sloth.example.configuration;

import com.tsingj.sloth.client.consumer.SlothRemoteConsumer;
import com.tsingj.sloth.client.producer.SlothRemoteProducer;
import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.ProducerProperties;
import com.tsingj.sloth.client.springsupport.RemoteProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import com.tsingj.sloth.example.listener.MessageOrderedListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.util.Map;


/**
 * @author yanghao
 */
@Configuration
@EnableConfigurationProperties(SlothClientProperties.class)
public class SlothConfiguration {

    private final SlothClientProperties slothClientProperties;

    public SlothConfiguration(SlothClientProperties slothClientProperties) {
        this.slothClientProperties = slothClientProperties;
    }


    @Autowired
    private MessageOrderedListener messageOrderedListener;

    @Bean(destroyMethod = "destroy")
    @ConditionalOnProperty(prefix = "spring.sloth.producer", name = "enabled", havingValue = "true")
    public SlothRemoteProducer slothProducer() {
        this.checkClientProperties(slothClientProperties);
        RemoteProperties remoteProperties = slothClientProperties.getRemote();
        ProducerProperties producerProperties = slothClientProperties.getProducer();
        SlothRemoteProducer slothRemoteProducer = new SlothRemoteProducer();
        slothRemoteProducer.setRemoteProperties(remoteProperties);
        slothRemoteProducer.setProducerProperties(producerProperties);
        slothRemoteProducer.start();
        return slothRemoteProducer;
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnProperty(prefix = "spring.sloth.consumer", value = "enabled", havingValue = "true")
    public SlothRemoteConsumer slothConsumer() {
        this.checkClientProperties(slothClientProperties);
        RemoteProperties remoteProperties = slothClientProperties.getRemote();
        Map<String, ConsumerProperties> consumerPropertiesMap = slothClientProperties.getConsumer().getConsumers();
        ConsumerProperties consumerProperties = consumerPropertiesMap.get("test-topic");
        Assert.notNull(consumerProperties, "Please check your config, topic config invalid!");
        SlothRemoteConsumer slothRemoteConsumer = new SlothRemoteConsumer();
        slothRemoteConsumer.setRemoteProperties(remoteProperties);
        slothRemoteConsumer.setConsumerProperties(consumerProperties);
        slothRemoteConsumer.registerListener(messageOrderedListener);
        slothRemoteConsumer.start();
        return slothRemoteConsumer;
    }

    private void checkClientProperties(SlothClientProperties slothClientProperties) {
        String brokerUrl = slothClientProperties.getRemote().getBrokerUrl();
        Assert.notNull(brokerUrl, "Please check your properties , brokerUrl is null!");
        String[] brokerUrlArr = brokerUrl.split(":");
        Assert.isTrue(brokerUrlArr.length == 2, "please check your brokerUrl! not expect [host:port] !");
    }

}
