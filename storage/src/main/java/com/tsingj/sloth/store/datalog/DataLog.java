package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.*;
import com.tsingj.sloth.store.utils.CompressUtil;
import com.tsingj.sloth.store.utils.CrcUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author yanghao
 */
@Component
public class DataLog {

    @Autowired
    private DataLogFileSet dataLogFileManager;

    public PutMessageResult putMessage(Message message) {

        //------------------补充并获取存储信息------------------
        //set 计算偏移量
        message.setOffset(1L);
        //set 存储时间
        message.setStoreTimestamp(System.currentTimeMillis());
        //set crc
        message.setCrc(CrcUtil.crc32(message.getBody()));
        //set version  1-127
        message.setVersion(CommonConstants.CURRENT_VERSION);
        //encode message
        byte[] encode = StoreEncoder.encode(message);
        if (encode == null) {
            return PutMessageResult.builder().status(PutMessageStatus.DATA_ENCODE_FAIL).build();
        }
        DataLogFile dataLogFile = null;
        //2、获取文件操作
        try {
            dataLogFile = dataLogFileManager.getLatestDataLogFile(message.getTopic(), message.getPartition(), encode.length);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (dataLogFile == null) {
            return PutMessageResult.builder().status(PutMessageStatus.LOG_FILE_OPERATION_FAIL).build();
        }
        try {
            dataLogFile.doAppend(encode);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //5、存储log

        return PutMessageResult.builder().status(PutMessageStatus.OK).build();
    }

    private static int calStoreLength(int bodyLen, int topicLen, int propertiesLen) {
        return DataLogConstants.KeyBytes.STORE +
                DataLogConstants.KeyBytes.STORE_TIMESTAMP +
                DataLogConstants.KeyBytes.VERSION +
                DataLogConstants.KeyBytes.TOPIC +
                topicLen +
                DataLogConstants.KeyBytes.PARTITiON +
                DataLogConstants.KeyBytes.PROPERTIES +
                propertiesLen +
                DataLogConstants.KeyBytes.CRC +
                DataLogConstants.KeyBytes.BODY_SIZE +
                bodyLen;
    }


    public static class StoreEncoder {

        public static byte[] encode(Message message) {
            //---------------基础属性----------------
            byte[] topic = message.getTopic().getBytes(StandardCharsets.UTF_8);
            byte topicLen = (byte) topic.length;
            int partitionId = message.getPartition();
            byte[] properties = messageProperties2String(message.getProperties()).getBytes(StandardCharsets.UTF_8);
            int propertiesLen = properties.length;
            //now used GZIP compress
            //todo get compress way in properties
            byte[] body = CompressUtil.GZIP.compress(message.getBody());
            if (body == null) {
                return null;
            }
            int bodyLen = body.length;
            //---------------存储附加属性----------------
            long offset = message.getOffset();
            long storeTimestamp = message.getStoreTimestamp();
            int crc = message.getCrc();
            byte version = message.getVersion();
            //this storeLen is after offset
            int storeLen = calStoreLength(bodyLen, topicLen, propertiesLen);

            //------ build append buffer ----
            ByteBuffer storeByteBuffer = ByteBuffer.allocate(DataLogConstants.KeyBytes.OFFSET + storeLen);
            //1、offset - 8
            storeByteBuffer.putLong(offset);
            //2、storeSize - 4
            storeByteBuffer.putInt(storeLen);
            //3、storeTimeStamp - 8
            storeByteBuffer.putLong(storeTimestamp);
            //4、version - 1
            storeByteBuffer.put(version);
            //5、topicLen - 1
            storeByteBuffer.put(topicLen);
            //6、topic - cal
            storeByteBuffer.put(topic);
            //7、partitionId - 4
            storeByteBuffer.putInt(partitionId);
            //8、propertiesLen - 4
            storeByteBuffer.putInt(propertiesLen);
            //9、properties - cal
            storeByteBuffer.put(properties);
            //10、crc - 4
            storeByteBuffer.putInt(crc);
            //11、msgSize - 4
            storeByteBuffer.putInt(bodyLen);
            //12、msgBody - cal
            storeByteBuffer.put(body);
            return storeByteBuffer.array();
        }

        private static String messageProperties2String(Map<String, String> properties) {
            StringBuilder sb = new StringBuilder();
            if (properties != null) {
                for (final Map.Entry<String, String> entry : properties.entrySet()) {
                    final String name = entry.getKey();
                    final String value = entry.getValue();

                    sb.append(name);
                    sb.append(CommonConstants.NAME_VALUE_SEPARATOR);
                    sb.append(value);
                    sb.append(CommonConstants.PROPERTY_SEPARATOR);
                }
            }
            return sb.toString();
        }

    }


    public static class StoreDecoder {

        public static Message decode(long offset, byte[] storeBytes) {
            return null;
        }

    }


}
