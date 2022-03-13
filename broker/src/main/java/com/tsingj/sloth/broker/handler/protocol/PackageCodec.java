package com.tsingj.sloth.broker.handler.protocol;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * @author yanghao
 */
@Slf4j
public class PackageCodec {

    public static DataPackage decode(ByteBuf byteBuf) {

        // Make sure if the length field was received.
        if (byteBuf.readableBytes() < ProtocolConstants.FieldLength.ALL) {
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

        long correlationId = byteBuf.readLong();

        byte version = byteBuf.readByte();

        byte command = byteBuf.readByte();

        int dataLen = byteBuf.readInt();

        // Make sure if there's enough bytes in the buffer.
        if (byteBuf.readableBytes() < dataLen) {
            // The whole bytes were not received yet - return null.
            // This method will be invoked again when more packets are
            // received and appended to the buffer.

            // Reset to the marked position to read the length field again
            // next time.
            byteBuf.resetReaderIndex();
            return null;
        }

        byte[] dataBytes = new byte[dataLen];
        byteBuf.readBytes(dataBytes);

        return new DataPackage(magicCode, correlationId, version, command, dataBytes);
    }

}
