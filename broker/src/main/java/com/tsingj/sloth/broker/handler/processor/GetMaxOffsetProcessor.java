package com.tsingj.sloth.broker.handler.processor;

import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.remoting.RemoteRequestProcessor;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.RemoteCommand;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import com.tsingj.sloth.store.StorageEngine;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class GetMaxOffsetProcessor implements RemoteRequestProcessor {

    private final StorageEngine storageEngine;

    public GetMaxOffsetProcessor(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    @Override
    public byte getCommand() {
        return ProtocolConstants.Command.GET_MAX_OFFSET;
    }

    @Override
    public RemoteCommand process(RemoteCommand request, ChannelHandlerContext ctx) throws Exception {
        log.debug("receive GET_MAX_OFFSET command.");
        Remoting.GetOffsetRequest getOffsetRequest = Remoting.GetOffsetRequest.parseFrom(request.getData());

        /*
         * check and set default param
         */
        String topic = getOffsetRequest.getTopic();
        if (ObjectUtils.isEmpty(topic)) {
            return this.respError(request, "IllegalArgument topic is empty!");
        }
        int partition = getOffsetRequest.getPartition();
        if (partition == 0) {
            return this.respError(request, "IllegalArgument partition is default 0!");
        }

        long offset = storageEngine.getMaxOffset(topic, partition);
        return this.respSuccess(request, offset);
    }

    private RemoteCommand respSuccess(RemoteCommand request, long offset) {
        Remoting.GetOffsetResult getOffsetResult = Remoting.GetOffsetResult.newBuilder()
                .setRetCode(Remoting.RetCode.SUCCESS)
                .setOffset(offset)
                .build();
        RemoteCommand response = request;
        response.setTimestamp(SystemClock.now());
        response.setData(getOffsetResult.toByteArray());
        return response;
    }

    private RemoteCommand respError(RemoteCommand request, String errMsg) {
        log.warn("process command GET_MAX_OFFSET fail! {}", errMsg);
        Remoting.GetOffsetResult getOffsetResult = Remoting.GetOffsetResult.newBuilder()
                .setRetCode(Remoting.RetCode.ERROR)
                .setErrorInfo(errMsg)
                .build();
        RemoteCommand response = request;
        response.setTimestamp(SystemClock.now());
        response.setData(getOffsetResult.toByteArray());
        return response;
    }


}
