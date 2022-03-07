package com.tsingj.sloth.store.log;


/**
 * @author yanghao
 */
public class IndexEntry {

    private long indexKey;

    private long indexValue;

    public long getIndexKey() {
        return indexKey;
    }

    public long getIndexValue() {
        return indexValue;
    }


    @Override
    public String toString() {
        return "IndexEntry{" +
                "indexKey=" + indexKey +
                ", indexValue=" + indexValue +
                '}';
    }


    public static class OffsetPosition extends IndexEntry {

        public OffsetPosition(long offset, long position) {
            super.indexKey = offset;
            super.indexValue = position;
        }

        public long getOffset() {
            return super.getIndexKey();
        }

        public long getPosition() {
            return this.getIndexValue();
        }

    }

    public static class TimestampOffset extends IndexEntry {

        public TimestampOffset(long timestamp, long offset) {
            super.indexKey = timestamp;
            super.indexValue = offset;
        }

        private long getTimestamp() {
            return super.indexKey;
        }

        private long getOffset() {
            return super.indexValue;
        }
    }

}
