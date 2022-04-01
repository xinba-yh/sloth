package com.tsingj.sloth.broker.event;

import com.tsingj.sloth.broker.constants.EventType;
import com.tsingj.sloth.broker.service.ConsumerGroupManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class AsyncEventListener implements ApplicationListener<AsyncEvent> {

    private final ConsumerGroupManager consumerGroupManager;

    public AsyncEventListener(ConsumerGroupManager consumerGroupManager) {
        this.consumerGroupManager = consumerGroupManager;
    }

    @Async(value = "asyncExecutor")
    @Override
    public void onApplicationEvent(AsyncEvent event) {
        if (event.getType() == EventType.CHANNEL_CLOSED) {
            log.debug("{} receive channel close event:{}.", Thread.currentThread().getName(), event.getMessage());
            this.processChannelClose(event.getMessage());
        }
    }

    private void processChannelClose(String clientId) {
        consumerGroupManager.removeClosedClient(clientId);
    }

}
