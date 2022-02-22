package com.tsingj.sloth.store.utils;

import java.util.zip.CRC32;

/**
 * @author yanghao
 */
public class CrcUtil {

    public static int crc32(byte[] array) {
        if (array != null) {
            CRC32 crc32 = new CRC32();
            crc32.update(array, 0, array.length);
            return (int) (crc32.getValue() & 0x7FFFFFFF);
        }
        return 0;
    }

}
