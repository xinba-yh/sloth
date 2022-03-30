package com.tsingj.sloth.client.springsupport;

import com.tsingj.sloth.client.SlothRemoteClient;
import com.tsingj.sloth.client.consumer.SlothConsumerManager;
import com.tsingj.sloth.client.producer.SlothRemoteProducer;
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

    private final SlothClientProperties slothClientProperties;

    public SlothConfiguration(SlothClientProperties slothClientProperties) {
        this.slothClientProperties = slothClientProperties;
    }

    @Bean(initMethod = "initConnect", destroyMethod = "closeConnect")
    public SlothRemoteClient slothRemoteClient(){
        this.checkClientProperties(slothClientProperties);
        SlothRemoteClient slothRemoteClient = new SlothRemoteClient(slothClientProperties);
        return slothRemoteClient;
    }

    @Bean
    public SlothRemoteProducer slothProducer(SlothRemoteClient slothRemoteClient) {
        SlothRemoteProducer slothProducer = new SlothRemoteProducer(slothClientProperties,slothRemoteClient);
        return slothProducer;
    }

//    @ConditionalOnProperty
    @Bean(initMethod = "start", destroyMethod = "close")
    public SlothConsumerManager slothConsumerManager(SlothRemoteClient slothRemoteClient) {
        SlothConsumerManager slothConsumerManager = new SlothConsumerManager(slothClientProperties,slothRemoteClient);
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
