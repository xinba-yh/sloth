package com.tsingj.sloth.store;

import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;

/**
 * @author yanghao
 */
@Data
@FieldDefaults(level = AccessLevel.PROTECTED)
public class IndexEntry {

    long indexKey;

    long indexValue;

    @Override
    public String toString() {
        return "IndexEntry{" +
                "indexKey=" + indexKey +
                ", indexValue=" + indexValue +
                '}';
    }


    @FieldDefaults(level = AccessLevel.PRIVATE)
    public static class OffsetPosition extends IndexEntry {

        public OffsetPosition(long offset, long position) {
            this.indexKey = offset;
            this.indexValue = position;
        }

    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    public static class TimestampOffset extends IndexEntry {

        public TimestampOffset(long timestamp, long offset) {
            this.indexKey = timestamp;
            this.indexValue = offset;
        }

    }

}
