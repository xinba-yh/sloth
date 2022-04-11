package com.tsingj.sloth.store.utils;

import com.tsingj.sloth.store.constants.CommonConstants;
import com.tsingj.sloth.store.constants.LogConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yanghao
 */
public class CommonUtil {

    private static final Logger logger = LoggerFactory.getLogger(CommonUtil.class);

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


    public static long hourToMills(int hour) {
        return hour * 60L * 60L * 1000L;
    }

    public static void deleteExpireFile(FileChannel fileChannel, File file) {
        try {
            try {
                fileChannel.close();
            } catch (Throwable t) {
                logger.error("Failed to close channel!", t);
            }
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            logger.warn("Failed to delete file {}! info:{}", file.getAbsolutePath(), e.getMessage());
        }
    }

    public static void delShutdownCleanFile(String shutdownCleanPath) {
        File file = new File(shutdownCleanPath);
        file.deleteOnExit();
    }


    public static boolean shutdownCleanFileExists(String shutdownCleanPath) {
        File file = new File(shutdownCleanPath);
        return file.exists();
    }

    public static boolean createShutdownCleanFile(String shutdownCleanPath) {
        File file = new File(shutdownCleanPath);
        try {
            if (!file.exists()) {
                return file.createNewFile();
            } else {
                logger.warn("Tmp shutdownClean file exists!");
                return false;
            }
        } catch (IOException e) {
            logger.error("Create tmp shutdownClean file fail!", e);
            return false;
        }
    }
}
