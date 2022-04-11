package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.common.result.Results;
import com.tsingj.sloth.store.constants.LogConstants;

import java.io.*;


/**
 * @author yanghao
 */
public class OffsetIndex extends AbstractIndex {

    public OffsetIndex(String logPath) throws FileNotFoundException {
        super(logPath);
    }

    @Override
    protected String indexType() {
        return LogConstants.FileSuffix.OFFSET_INDEX;
    }


    public Result<IndexEntry.OffsetPosition> getOffsetIndexFileLastOffset() {
        Result<IndexEntry> indexFileLastOffset = this.getIndexFileLastOffset();
        if (indexFileLastOffset.failure()) {
            return Results.failure(indexFileLastOffset.getMsg());
        }
        IndexEntry indexEntry = indexFileLastOffset.getData();
        return Results.success(new IndexEntry.OffsetPosition(indexEntry.getIndexKey(), indexEntry.getIndexValue()));
    }

}
