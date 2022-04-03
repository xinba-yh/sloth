package com.tsingj.sloth.client.producer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.client.RemoteCorrelationManager;
import com.tsingj.sloth.client.SlothRemoteClient;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yanghao
 */
@Slf4j
public class SlothRemoteProducer {

    private final SlothRemoteClient slothRemoteClient;

    private final SlothClientProperties clientProperties;

    public SlothRemoteProducer(SlothClientProperties clientProperties, SlothRemoteClient slothRemoteClient) {
        this.clientProperties = clientProperties;
        this.slothRemoteClient = slothRemoteClient;
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
        slothRemoteClient.getChannel().writeAndFlush(dataPackage);
    }

    public Remoting.SendResult send(Remoting.Message message) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
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
            slothRemoteClient.getChannel().writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} wait response null!", currentCorrelationId);
                return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("receive data null!").build();
            }
            byte[] data = responseData.getData();
            return Remoting.SendResult.parseFrom(data);
        } catch (InterruptedException e) {
            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.TIMEOUT).build();
        } catch (InvalidProtocolBufferException e) {
            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("protobuf parse error!" + e.getMessage()).build();
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
    }

}