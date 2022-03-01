package com.tsingj.sloth.store.utils;

import com.tsingj.sloth.store.DataLogConstants;

import java.text.NumberFormat;

/**
 * @author yanghao
 */
public class CommonUtil {

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static long fileName2Offset(String fileName) {
        String segmentFileName = fileName.replace(DataLogConstants.FileSuffix.LOG, "");
        return Long.parseLong(segmentFileName);
    }

}
