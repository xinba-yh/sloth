package com.tsingj.sloth.client.producer;

import com.tsingj.sloth.client.SlothRemoteClient;
import com.tsingj.sloth.client.SlothRemoteClientSingleton;
import com.tsingj.sloth.client.springsupport.ProducerProperties;
import com.tsingj.sloth.client.springsupport.RemoteProperties;
import com.tsingj.sloth.remoting.message.Remoting;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

/**
 * @author yanghao
 */
@Slf4j
public class SlothRemoteProducer {

    private SlothRemoteClient slothRemoteClient;

    private RemoteProperties remoteProperties;

    private ProducerProperties producerProperties;

    public void setRemoteProperties(RemoteProperties remoteProperties) {
        this.remoteProperties = remoteProperties;
    }


    public void setProducerProperties(ProducerProperties producerProperties) {
        this.producerProperties = producerProperties;
    }

    public void start() {
        this.slothRemoteClient = SlothRemoteClientSingleton.getInstance(this.remoteProperties);
    }


    //-----------------------remote call-----------------------

    public void sendOneway(Remoting.Message message) {
        //add check
        int messageMaxBytes = producerProperties.getMessageMaxBytes();
        Assert.isTrue(message.getBody().size() <= messageMaxBytes, "Message body to large! max message body:" + messageMaxBytes + "!");
        //send msg
        this.slothRemoteClient.sendOneway(message);
    }


    public Remoting.SendResult send(Remoting.Message message) {
        //add check
        int messageMaxBytes = producerProperties.getMessageMaxBytes();
        if (message.getBody().size() > messageMaxBytes) {
            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("Message body to large! max message body:" + messageMaxBytes + "!").build();
        }
        return this.slothRemoteClient.send(message, producerProperties.getTimeout(), producerProperties.getRetryTimes(), producerProperties.getRetryInterval());
    }

    private void destroy() {
        this.slothRemoteClient.closeConnect();
    }


}
