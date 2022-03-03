package com.tsingj.sloth.store.utils;

import com.tsingj.sloth.store.constants.CommonConstants;
import com.tsingj.sloth.store.constants.LogConstants;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yanghao
 */
public class CommonUtil {

    public static int calStoreLength(int bodyLen, int topicLen, int propertiesLen) {
        return LogConstants.MessageKeyBytes.STORE_TIMESTAMP +
                LogConstants.MessageKeyBytes.VERSION +
                LogConstants.MessageKeyBytes.TOPIC +
                topicLen +
                LogConstants.MessageKeyBytes.PARTITION +
                LogConstants.MessageKeyBytes.PROPERTIES +
                propertiesLen +
                LogConstants.MessageKeyBytes.CRC +
                LogConstants.MessageKeyBytes.BODY_SIZE +
                bodyLen;
    }

    public static Map<String, String> string2messageProperties(final String properties) {
        Map<String, String> map = new HashMap<>(1);
        if (properties != null) {
            String[] items = properties.split(CommonConstants.PROPERTY_SEPARATOR);
            for (String i : items) {
                String[] nv = i.split(CommonConstants.NAME_VALUE_SEPARATOR);
                if (2 == nv.length) {
                    map.put(nv[0], nv[1]);
                }
            }
        }
        return map;
    }

    public static String messageProperties2String(Map<String, String> properties) {
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

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static long fileName2Offset(String fileName) {
        String segmentFileName = fileName.replace(LogConstants.FileSuffix.LOG, "");
        return Long.parseLong(segmentFileName);
    }

}
