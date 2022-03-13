package com.tsingj.sloth.broker;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

/**
 * @author yanghao
 */
@ComponentScan(basePackages = "com.tsingj.sloth")
@SpringBootApplication
public class BrokerApplication {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(BrokerApplication.class, args);
        BrokerServer brokerServer = applicationContext.getBean(BrokerServer.class);
        brokerServer.start();
    }

}
