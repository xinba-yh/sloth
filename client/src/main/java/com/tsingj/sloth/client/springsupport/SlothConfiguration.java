package com.tsingj.sloth.client.springsupport;

import com.tsingj.sloth.client.SlothClientOptions;
import com.tsingj.sloth.client.SlothClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.Assert;

/**
 * @author yanghao
 */
@Configuration
@EnableConfigurationProperties(SlothClientProperties.class)
public class SlothConfiguration {

    @Autowired
    private SlothClientProperties slothClientProperties;

    @Autowired
    private ConfigurableEnvironment environment;

    @Bean(initMethod = "start",destroyMethod = "close")
    public SlothClient slothClient() {
        System.out.println(slothClientProperties);
        String brokerUrl = slothClientProperties.getBrokerUrl();
        Assert.notNull(brokerUrl, "Please check your properties , brokerUrl is null!");

        SlothClientOptions slothClientOptions = new SlothClientOptions(brokerUrl);
        slothClientOptions.setWorkGroupThreadSize(slothClientProperties.getWorkGroupThreadSize());
        slothClientOptions.setMaxSize(slothClientProperties.getMaxSize());
        slothClientOptions.setConnectTimeout(slothClientProperties.getConnectTimeout());
        slothClientOptions.setIoEventGroupType(slothClientProperties.getIoEventGroupType());
        slothClientOptions.setOnceTalkTimeout(slothClientProperties.getOnceTalkTimeout());
        SlothClient slothClient = new SlothClient(slothClientOptions);
        return slothClient;
    }

}
