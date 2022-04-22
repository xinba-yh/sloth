package com.tsingj.sloth.broker;


import com.tsingj.sloth.store.RaftReplicationServer;
import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.properties.StorageProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

/**
 * @author yanghao
 */
@Slf4j
@ComponentScan(basePackages = "com.tsingj.sloth")
@SpringBootApplication
public class BrokerApplication {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(BrokerApplication.class, args);
        StorageProperties storageProperties = applicationContext.getBean(StorageProperties.class);
        String startMode = storageProperties.getMode();
        log.info("Start sloth used {} mode.",startMode);
        if (startMode.equalsIgnoreCase(LogConstants.StoreMode.STANDALONE)) {
            //doNothing
        } else if (startMode.equals(LogConstants.StoreMode.RAFT)) {
            RaftReplicationServer raftReplicationServer = applicationContext.getBean(RaftReplicationServer.class);
            raftReplicationServer.start();
        } else {
            throw new UnsupportedOperationException("Unsupported start mode " + startMode + " !");
        }
        BrokerServer brokerServer = applicationContext.getBean(BrokerServer.class);
        brokerServer.start();
    }

}
