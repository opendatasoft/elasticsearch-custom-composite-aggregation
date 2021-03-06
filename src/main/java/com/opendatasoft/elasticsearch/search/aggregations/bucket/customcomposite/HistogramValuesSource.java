package com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

class HistogramValuesSource extends ValuesSource.Numeric {
    private final Numeric vs;
    private final double interval;

    /**
     *
     * @param vs The original values source
     */
    HistogramValuesSource(Numeric vs, double interval) {
        this.vs = vs;
        this.interval = interval;
    }

    @Override
    public boolean isFloatingPoint() {
        return true;
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
        SortedNumericDoubleValues values = vs.doubleValues(context);
        return new SortedNumericDoubleValues() {
            @Override
            public double nextValue() throws IOException {
                return Math.floor(values.nextValue() / interval) * interval;
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }
        };
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }
}
