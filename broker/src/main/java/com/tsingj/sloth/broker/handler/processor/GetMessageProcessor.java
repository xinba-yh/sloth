package com.tsingj.sloth.broker.handler.processor;

import com.google.protobuf.ByteString;
import com.tsingj.sloth.remoting.RemoteRequestProcessor;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.*;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class GetMessageProcessor implements RemoteRequestProcessor {

    private final StorageEngine storageEngine;

    public GetMessageProcessor(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    @Override
    public byte getCommand() {
        return ProtocolConstants.Command.GET_MESSAGE;
    }

    @Override
    public DataPackage process(DataPackage request, ChannelHandlerContext ctx) throws Exception {
        log.debug("receive GET_MESSAGE command.");
        Remoting.GetMessageRequest getMessageRequest = Remoting.GetMessageRequest.parseFrom(request.getData());

        /*
         * check and set default param
         */
        String topic = getMessageRequest.getTopic();
        if (ObjectUtils.isEmpty(topic)) {
            return this.respError(request, "IllegalArgument topic is empty!");
        }
        int partition = getMessageRequest.getPartition();
        if (partition == 0) {
            return this.respError(request, "IllegalArgument partition is default 0!");
        }
        long offset = getMessageRequest.getOffset();

        /*
         * get message
         */
        GetMessageResult getMessageResult = storageEngine.getMessage(topic, partition, offset);
        if (getMessageResult.getStatus() == GetMessageStatus.FOUND) {
            return this.respFound(request, getMessageResult.getMessage());
        } else if (getMessageResult.getStatus() == GetMessageStatus.OFFSET_NOT_FOUND || getMessageResult.getStatus() == GetMessageStatus.LOG_SEGMENT_NOT_FOUND || getMessageResult.getStatus() == GetMessageStatus.PARTITION_NO_MESSAGE) {
            return this.respNotFound(request, getMessageResult.getErrorMsg());
        } else {
            return this.respError(request, getMessageResult.getStatus() + ":" + getMessageResult.getErrorMsg());
        }
    }

    private DataPackage respFound(DataPackage request, Message message) {
        Remoting.GetMessageResult.Message respMessage = Remoting.GetMessageResult.Message.newBuilder()
                .setTopic(message.getTopic())
                .setPartition(message.getPartition())
                .putAllProperties(message.getProperties())
                .setBody(ByteString.copyFrom(message.getBody()))
                .setOffset(message.getOffset())
                .setStoreTimestamp(message.getStoreTimestamp())
                .setCrc(message.getCrc())
                .setVersion(message.getVersion())
                .build();
        Remoting.GetMessageResult sendResult = Remoting.GetMessageResult.newBuilder()
                .setRetCode(Remoting.GetMessageResult.RetCode.FOUND)
                .setMessage(respMessage)
                .build();
        DataPackage response = request;
        response.setTimestamp(System.currentTimeMillis());
        response.setData(sendResult.toByteArray());
        return response;
    }

    private DataPackage respError(DataPackage request, String errMsg) {
        log.warn("process command GET_MESSAGE fail! {}", errMsg);
        Remoting.SendResult sendResult = Remoting.SendResult.newBuilder()
                .setRetCode(Remoting.SendResult.RetCode.ERROR)
                .setErrorInfo(errMsg)
                .build();
        DataPackage response = request;
        response.setTimestamp(System.currentTimeMillis());
        response.setData(sendResult.toByteArray());
        return response;
    }

    private DataPackage respNotFound(DataPackage request, String errorMsg) {
        Remoting.GetMessageResult sendResult = Remoting.GetMessageResult.newBuilder()
                .setRetCode(Remoting.GetMessageResult.RetCode.NOT_FOUND)
                .setErrorInfo(errorMsg != null ? errorMsg : "")
                .build();
        DataPackage response = request;
        response.setTimestamp(System.currentTimeMillis());
        response.setData(sendResult.toByteArray());
        return response;
    }

}
