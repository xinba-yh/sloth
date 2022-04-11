package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.constants.LogConstants;

import java.io.*;

/**
 * @author yanghao
 */
public class TimeIndex extends AbstractIndex {

    public TimeIndex(String logPath) throws FileNotFoundException {
        super(logPath);
    }

    @Override
    protected String indexType() {
        return LogConstants.FileSuffix.TIMESTAMP_INDEX;
    }

}
