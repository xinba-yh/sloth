package com.tsingj.sloth.client.producer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.client.RemoteCorrelationManager;
import com.tsingj.sloth.client.SlothRemoteClient;
import com.tsingj.sloth.client.SlothRemoteClientSingleton;
import com.tsingj.sloth.client.springsupport.ProducerProperties;
import com.tsingj.sloth.client.springsupport.RemoteProperties;
import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.common.result.Results;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
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


    //----------------------------------------------

    public void sendOneway(Remoting.Message message) {
        DataPackage dataPackage = DataPackage.builder()
                .magicCode(ProtocolConstants.MAGIC_CODE)
                .version(ProtocolConstants.VERSION)
                .command(ProtocolConstants.Command.SEND_MESSAGE)
                .requestType(ProtocolConstants.RequestType.ONE_WAY)
                .timestamp(SystemClock.now())
                .data(message.toByteArray())
                .build();
        this.slothRemoteClient.getChannel().writeAndFlush(dataPackage);
    }


    public Remoting.SendResult send(Remoting.Message message) {
        //add check
        int messageMaxBytes = producerProperties.getMessageMaxBytes();
        if (message.getBody().size() > messageMaxBytes) {
            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("Message body to large! max message body:" + messageMaxBytes + "!").build();
        }
        //add retry
        int retryTimes = 0;
        int maxRetryTimes = producerProperties.getRetryTimes();
        int retryInterval = producerProperties.getRetryInterval();
        Result<Remoting.SendResult> result;
        do {
            result = sendMessage(message);
            if (result.failure()) {
                retryTimes++;
                if (retryTimes > maxRetryTimes) {
                    break;
                }
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException ignored) {
                }
            }
        } while (result.failure());
        //无论成功或者失败，都将返回给上游处理，这里仅仅是替代上游做了重试。
        return result.getData();

    }

    private Result<Remoting.SendResult> sendMessage(Remoting.Message message) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.remoteProperties.getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.SEND_MESSAGE)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(SystemClock.now())
                    .data(message.toByteArray())
                    .build();

            //send data
            this.slothRemoteClient.getChannel().writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} wait response null!", currentCorrelationId);
                return Results.failure(null, null, Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("receive data null!").build());
            }
            byte[] data = responseData.getData();
            return Results.success(Remoting.SendResult.parseFrom(data));
        } catch (InterruptedException e) {
            return Results.failure(null, null, Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.TIMEOUT).build());
        } catch (InvalidProtocolBufferException e) {
            return Results.failure(null, null, Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("protobuf parse error!" + e.getMessage()).build());
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
    }

    private void destroy() {
        this.slothRemoteClient.closeConnect();
    }


}
