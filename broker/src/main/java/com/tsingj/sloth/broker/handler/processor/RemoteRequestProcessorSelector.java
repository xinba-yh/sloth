package com.tsingj.sloth.broker.handler.processor;

import com.tsingj.sloth.remoting.RemoteRequestProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class RemoteRequestProcessorSelector {

    private final static Map<Byte, RemoteRequestProcessor> REMOTE_REQUEST_PROCESSOR_MAP = new ConcurrentHashMap<>();

    @Autowired
    public RemoteRequestProcessorSelector(Map<String, RemoteRequestProcessor> syncStrategyMap) {
        log.info("prepare register remoteRequestProcessor.");
        syncStrategyMap.forEach((k, v) -> REMOTE_REQUEST_PROCESSOR_MAP.put(v.getCommand(), v));
        log.info("register remoteRequestProcessor done. {}", REMOTE_REQUEST_PROCESSOR_MAP.keySet());
    }

    public static RemoteRequestProcessor select(byte command) {
        RemoteRequestProcessor remoteRequestProcessor = REMOTE_REQUEST_PROCESSOR_MAP.get(command);
        Assert.notNull(remoteRequestProcessor, "unsupported [" + command + "] processor!");
        return remoteRequestProcessor;
    }

}
