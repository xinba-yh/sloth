package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.remoting.message.Remoting;


/**
 * @author yanghao
 */
public interface MessageListener {

    ConsumerStatus consumeMessage(final Remoting.GetMessageResult.Message msg);

}
