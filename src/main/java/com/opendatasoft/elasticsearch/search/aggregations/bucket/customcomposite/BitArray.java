package com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;

/**
 * A bit array that is implemented using a growing {@link LongArray}
 * created from {@link BigArrays}.
 * The underlying long array grows lazily based on the biggest index
 * that needs to be set.
 */
final class BitArray implements Releasable {
    private final BigArrays bigArrays;
    private LongArray bits;

    BitArray(BigArrays bigArrays, int initialSize) {
        this.bigArrays = bigArrays;
        this.bits = bigArrays.newLongArray(initialSize, true);
    }

    public void set(int index) {
        fill(index, true);
    }

    public void clear(int index) {
        fill(index, false);
    }

    public boolean get(int index) {
        int wordNum = index >> 6;
        long bitmask = 1L << index;
        return (bits.get(wordNum) & bitmask) != 0;
    }

    private void fill(int index, boolean bit) {
        int wordNum = index >> 6;
        bits = bigArrays.grow(bits,wordNum+1);
        long bitmask = 1L << index;
        long value = bit ? bits.get(wordNum) | bitmask : bits.get(wordNum) & ~bitmask;
        bits.set(wordNum, value);
    }

    @Override
    public void close() {
        Releasables.close(bits);
    }
}
