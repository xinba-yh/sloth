package com.tsingj.sloth.client.springsupport;

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

    @Bean(initMethod = "start",destroyMethod = "close")
    public SlothProducer slothProducer() {
        System.out.println(slothClientProperties);
        String brokerUrl = slothClientProperties.getBrokerUrl();
        Assert.notNull(brokerUrl, "Please check your properties , brokerUrl is null!");
        String[] brokerUrlArr = brokerUrl.split(":");
        Assert.isTrue(brokerUrlArr.length == 2, "please check your brokerUrl! not expect [host:port] !¬");
        SlothProducer slothProducer = new SlothProducer(slothClientProperties);
        return slothProducer;
    }

}
