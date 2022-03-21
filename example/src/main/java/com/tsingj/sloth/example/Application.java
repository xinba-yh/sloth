package com.tsingj.sloth.example;

import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        ConfigurableApplicationContext run = SpringApplication.run(Application.class, args);
        run.getEnvironment();
        SlothClientProperties bean = run.getBean(SlothClientProperties.class);
        System.out.println(bean);
    }

}
