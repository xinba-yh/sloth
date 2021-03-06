package com.tsingj.sloth.remoting.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * @author yanghao
 * 自定义协议
 * | Head | Meta | Data
 * --Head
 * magic_code      5字节
 * version         1字节
 * command         1字节
 * total_size      4字节
 * --Meta
 * send_type        1字节  //oneway 1 | sync 2
 * correlation_id   8字节
 * create_timestamp 8字节
 * --Data
 * data_len         4字节
 * data             N字节  //protobuf bytes
 */
@Slf4j
public class PackageCodec {

    //已在上层完成TCP粘包
    public static RemoteCommand decode(ByteBuf byteBuf) {

        // Make sure if the length field was received.
        if (byteBuf.readableBytes() < ProtocolConstants.FieldLength.HEAD_ALL) {
            log.info("buf is not completed");
            return null;
        }

        byteBuf.markReaderIndex();


        //magicCode
        byte[] magicCodeBytes = new byte[ProtocolConstants.FieldLength.MAGIC_CODE];
        byteBuf.readBytes(magicCodeBytes);

        String magicCode = new String(magicCodeBytes, StandardCharsets.UTF_8);
        if (!ProtocolConstants.MAGIC_CODE.equals(magicCode)) {
            throw new RuntimeException("Error magic code:" + magicCode);
        }

        byte version = byteBuf.readByte();

        byte command = byteBuf.readByte();

        int totalLen = byteBuf.readInt();

        // Make sure if there's enough bytes in the buffer.
        if (byteBuf.readableBytes() < totalLen) {
            // The whole bytes were not received yet - return null.
            // This method will be invoked again when more packets are
            // received and appended to the buffer.

            // Reset to the marked position to read the length field again
            // next time.
            byteBuf.resetReaderIndex();
            return null;
        }

        byte requestType = byteBuf.readByte();
        Long correlationId = null;
        if (requestType == ProtocolConstants.RequestType.SYNC) {
            correlationId = byteBuf.readLong();
        }
        long timestamp = byteBuf.readLong();

        int dataLen = byteBuf.readInt();
        byte[] dataBytes = new byte[dataLen];
        byteBuf.readBytes(dataBytes);

        return RemoteCommand.builder()
                .magicCode(magicCode)
                .version(version)
                .command(command)
                .requestType(requestType)
                .correlationId(correlationId)
                .timestamp(timestamp)
                .data(dataBytes)
                .build();
    }

    public static ByteBuf encode(RemoteCommand remoteCommand) {

        int headLen = ProtocolConstants.FieldLength.HEAD_ALL;
        int metaLen = ProtocolConstants.FieldLength.REQUEST_TYPE + (remoteCommand.getRequestType() == ProtocolConstants.RequestType.ONE_WAY ? 0 : ProtocolConstants.FieldLength.CORRELATION_ID) + ProtocolConstants.FieldLength.TIMESTAMP;
        int dataLen = ProtocolConstants.FieldLength.DATA_LEN + remoteCommand.getData().length;

        int totalLen = headLen + metaLen + dataLen;
        ByteBuf byteBuf = Unpooled.buffer(totalLen, totalLen);

        byte[] magicCodeBytes = ProtocolConstants.MAGIC_CODE.getBytes(StandardCharsets.UTF_8);
        byteBuf.writeBytes(magicCodeBytes);
        byteBuf.writeByte(remoteCommand.getVersion());
        byteBuf.writeByte(remoteCommand.getCommand());
        byteBuf.writeInt(metaLen + dataLen);

        byteBuf.writeByte(remoteCommand.getRequestType());
        if(remoteCommand.getRequestType() == ProtocolConstants.RequestType.SYNC){
            byteBuf.writeLong(remoteCommand.getCorrelationId());
        }
        byteBuf.writeLong(remoteCommand.getTimestamp());
        byteBuf.writeInt(remoteCommand.getData().length);
        byteBuf.writeBytes(remoteCommand.getData());

        return byteBuf;
    }

}
