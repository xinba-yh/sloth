package com.tsingj.sloth.client.springsupport;

import com.tsingj.sloth.client.consumer.SlothConsumerManager;
import com.tsingj.sloth.client.producer.SlothProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;


/**
 * @author yanghao
 */
@Configuration
@EnableConfigurationProperties(SlothClientProperties.class)
public class SlothConfiguration {

    @Autowired
    private SlothClientProperties slothClientProperties;

    @Bean(initMethod = "start", destroyMethod = "close")
    public SlothProducer slothProducer() {
        this.checkClientProperties(slothClientProperties);
        SlothProducer slothProducer = new SlothProducer(slothClientProperties);
        return slothProducer;
    }

//    @ConditionalOnProperty
    @Bean(initMethod = "start", destroyMethod = "close")
    public SlothConsumerManager slothConsumerManager() {
        this.checkClientProperties(slothClientProperties);
        SlothConsumerManager slothConsumerManager = new SlothConsumerManager(slothClientProperties);
        return slothConsumerManager;
    }

    private void checkClientProperties(SlothClientProperties slothClientProperties) {
        System.out.println(slothClientProperties);
        String brokerUrl = slothClientProperties.getBrokerUrl();
        Assert.notNull(brokerUrl, "Please check your properties , brokerUrl is null!");
        String[] brokerUrlArr = brokerUrl.split(":");
        Assert.isTrue(brokerUrlArr.length == 2, "please check your brokerUrl! not expect [host:port] !¬");
    }

}
