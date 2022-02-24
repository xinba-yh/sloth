package com.tsingj.sloth.store.utils;

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

}
