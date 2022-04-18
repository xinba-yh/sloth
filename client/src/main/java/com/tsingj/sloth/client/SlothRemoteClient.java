package com.tsingj.sloth.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.client.springsupport.CommonConstants;
import com.tsingj.sloth.client.springsupport.RemoteProperties;
import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.common.exception.ClientConnectException;
import com.tsingj.sloth.common.exception.ClientConnectTimeoutException;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.common.result.Results;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import com.tsingj.sloth.remoting.protocol.RemoteCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author yanghao
 */
@Slf4j
public class SlothRemoteClient {

    /**
     * The rpc client options.
     */
    private final RemoteProperties remoteProperties;

    /**
     * The worker group.
     */
    private EventLoopGroup workerGroup;

    /**
     * The Constant CLIENT_T_NAME.
     */
    private final static String POLL_NAME = "sloth";

    private final Bootstrap bootstrap = new Bootstrap();

    private volatile Channel channel;

    private String clientId;

    private final Object lock = new Object();

    private volatile boolean stopped = false;


    public SlothRemoteClient(RemoteProperties remoteProperties) {
        this.remoteProperties = remoteProperties;
        this.initConnect();

    }

    public void initConnect() {
        if (this.remoteProperties.getIoEventGroupType() == CommonConstants.EventGroupMode.POLL_EVENT_GROUP) {
            this.workerGroup = new NioEventLoopGroup(this.remoteProperties.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(POLL_NAME));
        } else {
            this.workerGroup = new EpollEventLoopGroup(this.remoteProperties.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(POLL_NAME));
        }

        this.bootstrap.group(this.workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, this.remoteProperties.getReuseAddress())
                .option(ChannelOption.SO_KEEPALIVE, this.remoteProperties.getKeepAlive())
                .option(ChannelOption.TCP_NODELAY, this.remoteProperties.getTcpNoDelay())
                .option(ChannelOption.SO_SNDBUF, this.remoteProperties.getSndBufSize())
                .option(ChannelOption.SO_RCVBUF, this.remoteProperties.getRcvBufSize())
                .handler(new RemoteClientChannelInitializer(this.remoteProperties.getMaxSize()));
        try {
            String[] brokerUrlArr = this.remoteProperties.getBrokerUrl().split(":");
            ChannelFuture channelFuture = bootstrap.connect(brokerUrlArr[0], Integer.parseInt(brokerUrlArr[1]));
            this.channel = channelFuture.sync().channel();
            this.clientId = UUID.randomUUID().toString();

        } catch (Throwable e) {
            throw new ClientConnectException("sloth client channel connect fail!");
        }
    }

    public void closeConnect() {
        this.stopped = true;

        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }

        if (this.channel != null) {
            this.channel.close().syncUninterruptibly();
        }
    }

    public Channel getChannel() {
        if (!this.channel.isActive() && !this.stopped) {
            synchronized (lock) {
                if (!this.channel.isActive() && !this.stopped) {
                    log.debug("thread:{} channel unActive! try reconnect!", Thread.currentThread().getId());
                    Integer connectTimeout = this.remoteProperties.getConnectTimeout();
                    String[] brokerUrlArr = this.remoteProperties.getBrokerUrl().split(":");
                    ChannelFuture channelFuture = this.bootstrap.connect(brokerUrlArr[0], Integer.parseInt(brokerUrlArr[1]));
                    //awaitUninterruptibly 底层为有锁。
                    if (channelFuture.awaitUninterruptibly(connectTimeout, TimeUnit.MILLISECONDS)) {
                        if (channelFuture.channel() != null && channelFuture.channel().isActive()) {
                            this.channel = channelFuture.channel();
                            log.info("sloth client channel reconnect success.");
                        } else {
                            throw new ClientConnectException("sloth client channel connect fail!");
                        }
                    } else {
                        throw new ClientConnectTimeoutException("sloth client channel connect timeout " + connectTimeout + " ms!");
                    }
                }
            }
        }
        return this.channel;
    }

    public String getClientId() {
        return this.clientId;
    }

    /**
     * sendCommand sync mode & wait response
     * @param remoteCommand
     * @param timeout
     * @return
     */
    private byte[] sendCommandSync(RemoteCommand remoteCommand, int timeout) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, timeout);
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            remoteCommand.setCorrelationId(currentCorrelationId);
            //send command
            this.getChannel().writeAndFlush(remoteCommand);
            RemoteCommand responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} sendCommand {} wait response null!", currentCorrelationId, remoteCommand.getCommand());
                return null;
            }
            return responseData.getData();
        } catch (InterruptedException e) {
            log.warn("correlationId {} sendCommand {} wait response interrupted! {}", currentCorrelationId, remoteCommand.getCommand(), e.getMessage());
            return null;
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
    }

    //----------------------client to broker--------------------------

    //----------------------producer message--------------------------

    public void sendOneway(Remoting.Message message) {
        RemoteCommand remoteCommand = RemoteCommand.builder()
                .magicCode(ProtocolConstants.MAGIC_CODE)
                .version(ProtocolConstants.VERSION)
                .command(ProtocolConstants.Command.SEND_MESSAGE)
                .requestType(ProtocolConstants.RequestType.ONE_WAY)
                .timestamp(SystemClock.now())
                .data(message.toByteArray())
                .build();
        this.getChannel().writeAndFlush(remoteCommand);
    }


    public Remoting.SendResult send(Remoting.Message message, int onceTimeout, int maxRetryTimes, int retryInterval) {
        //add retry
        int retryTimes = 0;
        Result<Remoting.SendResult> result;
        do {
            result = sendMessage(message, onceTimeout);
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

    private Result<Remoting.SendResult> sendMessage(Remoting.Message message, int onceTimeout) {
        try {
            RemoteCommand remoteCommand = RemoteCommand.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.SEND_MESSAGE)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .timestamp(SystemClock.now())
                    .data(message.toByteArray())
                    .build();

            //send command to remote broker
            byte[] data = this.sendCommandSync(remoteCommand, onceTimeout);
            return Results.success(Remoting.SendResult.parseFrom(data));
        } catch (InvalidProtocolBufferException e) {
            return Results.failure(null, null, Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("protobuf parse error!" + e.getMessage()).build());
        }
    }


    //----------------------consumer message--------------------------

    public List<Integer> heartbeat(String groupName, String topic, Integer maxConsumePartitions) {
        try {
            Remoting.ConsumerHeartbeatRequest consumerHeartbeatRequest = Remoting.ConsumerHeartbeatRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setClientId(this.getClientId())
                    .build();
            RemoteCommand remoteCommand = RemoteCommand.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.CONSUMER_GROUP_HEARTBEAT)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .timestamp(SystemClock.now())
                    .data(consumerHeartbeatRequest.toByteArray())
                    .build();

            //send command to remote broker
            byte[] data = this.sendCommandSync(remoteCommand, this.remoteProperties.getOnceTalkTimeout());
            Remoting.ConsumerHeartbeatResult consumerHeartbeatResult = Remoting.ConsumerHeartbeatResult.parseFrom(data);
            if (consumerHeartbeatResult.getRetCode() == Remoting.RetCode.ERROR) {
                log.warn("correlationId {} CONSUMER_GROUP_HEARTBEAT got error! {} ", remoteCommand.getCorrelationId(), consumerHeartbeatResult.getErrorInfo());
                return null;
            }
            List<Integer> topicPartitions = consumerHeartbeatResult.getPartitionsList();
            //根据配置的maxConsumePartitions截断broker分配的partitions
            if (topicPartitions.size() > maxConsumePartitions) {
                topicPartitions = topicPartitions.parallelStream().limit(maxConsumePartitions).collect(Collectors.toList());
            }
            return topicPartitions;
        } catch (InvalidProtocolBufferException e) {
            log.warn("CONSUMER_GROUP_HEARTBEAT protobuf parse error!" + e.getMessage());
        }
        return null;
    }

    public Long getConsumerOffset(String groupName, String topic, int partition) {
        try {
            Remoting.GetConsumerOffsetRequest consumerHeartbeatRequest = Remoting.GetConsumerOffsetRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setPartition(partition)
                    .build();
            RemoteCommand remoteCommand = RemoteCommand.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_CONSUMER_GROUP_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .timestamp(SystemClock.now())
                    .data(consumerHeartbeatRequest.toByteArray())
                    .build();

            //send command to remote broker
            byte[] data = this.sendCommandSync(remoteCommand, this.remoteProperties.getOnceTalkTimeout());
            Remoting.GetConsumerOffsetResult getConsumerOffsetResult = Remoting.GetConsumerOffsetResult.parseFrom(data);
            if (getConsumerOffsetResult.getRetCode() == Remoting.RetCode.ERROR) {
                log.warn("correlationId {} GET_CONSUMER_GROUP_OFFSET got error! {} ", remoteCommand.getCorrelationId(), getConsumerOffsetResult.getErrorInfo());
                return null;
            }
            return getConsumerOffsetResult.getOffset();
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_CONSUMER_GROUP_OFFSET protobuf parse error!" + e.getMessage());
        }
        return null;
    }

    public Long getMinOffset(String topic, Integer partition) {
        try {
            Remoting.GetOffsetRequest getOffsetRequest = Remoting.GetOffsetRequest.newBuilder()
                    .setTopic(topic)
                    .setPartition(partition)
                    .build();
            RemoteCommand remoteCommand = RemoteCommand.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_MIN_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .timestamp(SystemClock.now())
                    .data(getOffsetRequest.toByteArray())
                    .build();
            //send command to remote broker
            byte[] data = this.sendCommandSync(remoteCommand, this.remoteProperties.getOnceTalkTimeout());
            Remoting.GetOffsetResult getOffsetResult = Remoting.GetOffsetResult.parseFrom(data);
            if (getOffsetResult.getRetCode() == Remoting.RetCode.ERROR) {
                log.warn("correlationId {} GET_MIN_OFFSET got error! {} ", remoteCommand.getCorrelationId(), getOffsetResult.getErrorInfo());
                return null;
            }
            return getOffsetResult.getOffset();
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_MIN_OFFSET protobuf parse error!" + e.getMessage());
        }
        return null;
    }

    public Long getMaxOffset(String topic, Integer partition) {
        try {
            Remoting.GetOffsetRequest getOffsetRequest = Remoting.GetOffsetRequest.newBuilder()
                    .setTopic(topic)
                    .setPartition(partition)
                    .build();
            RemoteCommand remoteCommand = RemoteCommand.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_MAX_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .timestamp(SystemClock.now())
                    .data(getOffsetRequest.toByteArray())
                    .build();
            //send command to remote broker
            byte[] data = this.sendCommandSync(remoteCommand, this.remoteProperties.getOnceTalkTimeout());
            Remoting.GetOffsetResult getOffsetResult = Remoting.GetOffsetResult.parseFrom(data);
            if (getOffsetResult.getRetCode() == Remoting.RetCode.ERROR) {
                log.warn("correlationId {} GET_MAX_OFFSET got error! {} ", remoteCommand.getCorrelationId(), getOffsetResult.getErrorInfo());
                return null;
            }
            return getOffsetResult.getOffset();
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_MAX_OFFSET protobuf parse error!" + e.getMessage());
        }
        return null;
    }


    public Remoting.GetMessageResult fetchMessage(String topic, int partition, long offset) {
        try {
            Remoting.GetMessageRequest getMessageRequest = Remoting.GetMessageRequest.newBuilder()
                    .setTopic(topic)
                    .setPartition(partition)
                    .setOffset(offset)
                    .build();
            RemoteCommand remoteCommand = RemoteCommand.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_MESSAGE)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .timestamp(SystemClock.now())
                    .data(getMessageRequest.toByteArray())
                    .build();
            //send command to remote broker
            byte[] data = this.sendCommandSync(remoteCommand, this.remoteProperties.getOnceTalkTimeout());
            return Remoting.GetMessageResult.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            log.warn("fetchMessage protobuf parse error!" + e.getMessage());
        }
        return null;
    }


    public Remoting.SubmitConsumerOffsetResult submitOffset(String groupName, String topic, Integer partition, long offset) {
        try {
            Remoting.SubmitConsumerOffsetRequest getMessageRequest = Remoting.SubmitConsumerOffsetRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setPartition(partition)
                    .setOffset(offset)
                    .build();
            RemoteCommand remoteCommand = RemoteCommand.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.SUBMIT_CONSUMER_GROUP_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .timestamp(SystemClock.now())
                    .data(getMessageRequest.toByteArray())
                    .build();
            //send command to remote broker
            byte[] data = this.sendCommandSync(remoteCommand, this.remoteProperties.getOnceTalkTimeout());
            return Remoting.SubmitConsumerOffsetResult.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_MESSAGE protobuf parse error!" + e.getMessage());
        }
        return null;
    }


}
