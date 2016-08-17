package com.nexr.lean.kafka.common;

public class OffsetInfo {

    private final long committed;
    private final long end;

    public OffsetInfo(long committed, long end) {
        this.committed = committed;
        this.end = end;
    }

    /**
     * Gets the last committed offset.
     *
     * @return if there is no committed offset, return -1.
     */
    public long getCommitted() {
        return committed;
    }

    public long getEnd() {
        return end;
    }

}
