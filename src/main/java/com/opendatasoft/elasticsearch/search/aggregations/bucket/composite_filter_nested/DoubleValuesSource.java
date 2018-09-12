package com.opendatasoft.elasticsearch.search.aggregations.bucket.composite_filter_nested;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

/**
 * A {@link SingleDimensionValuesSource} for doubles.
 */
class DoubleValuesSource extends SingleDimensionValuesSource<Double> {
    private final CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesFunc;
    private final DoubleArray values;
    private double currentValue;

    DoubleValuesSource(BigArrays bigArrays, MappedFieldType fieldType,
                       CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesFunc,
                       DocValueFormat format, Object missing, int size, int reverseMul, Weight weight,
                       ObjectMapper childObjectMapper, BitSetProducer parentFilter) {
        super(format, fieldType, missing, size, reverseMul, weight, childObjectMapper, parentFilter);
        this.docValuesFunc = docValuesFunc;
        this.values = bigArrays.newDoubleArray(size, false);
    }

    @Override
    void copyCurrent(int slot) {
        values.set(slot, currentValue);
    }

    @Override
    int compare(int from, int to) {
        return compareValues(values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        return compareValues(currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        return compareValues(currentValue, afterValue);
    }

    private int compareValues(double v1, double v2) {
        return Double.compare(v1, v2) * reverseMul;
    }

    @Override
    void setAfter(Comparable<?> value) {
        if (value instanceof Number) {
            afterValue = ((Number) value).doubleValue();
        } else {
            afterValue = format.parseDouble(value.toString(), false, () -> {
                throw new IllegalArgumentException("now() is not supported in [after] key");
            });
        }
    }

    @Override
    Double toComparable(int slot) {
        return values.get(slot);
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedNumericDoubleValues dvs = docValuesFunc.apply(context);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        currentValue = dvs.nextValue();
                        next.collect(doc, bucket);
                    }
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable<?> value, LeafReaderContext context, LeafBucketCollector next) {
        if (value.getClass() != Double.class) {
            throw new IllegalArgumentException("Expected Double, got " + value.getClass());
        }
        currentValue = (Double) value;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                next.collect(doc, bucket);
            }
        };
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        return null;
    }

    @Override
    public void close() {
        Releasables.close(values);
    }
}
