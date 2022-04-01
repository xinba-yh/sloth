package com.tsingj.sloth.broker.event;

import com.tsingj.sloth.broker.constants.EventType;
import org.springframework.context.ApplicationEvent;

/**
 * @author yanghao
 */
public class AsyncEvent extends ApplicationEvent {

    private EventType type;

    private String message;

    public AsyncEvent(Object source, EventType type, String message) {
        super(source);
        this.type = type;
        this.message = message;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
