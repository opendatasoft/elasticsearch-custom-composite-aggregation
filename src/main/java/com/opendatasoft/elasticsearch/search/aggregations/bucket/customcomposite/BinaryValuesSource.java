package com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * A {@link SingleDimensionValuesSource} for binary source ({@link BytesRef}).
 */
class BinaryValuesSource extends SingleDimensionValuesSource<BytesRef> {
    private final LongConsumer breakerConsumer;
    private final CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesFunc;
    private ObjectArray<BytesRef> values;
    private ObjectArray<BytesRefBuilder> valueBuilders;
    private BytesRef currentValue;

    BinaryValuesSource(BigArrays bigArrays, LongConsumer breakerConsumer, MappedFieldType fieldType, CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesFunc,
                       DocValueFormat format, boolean missingBucket, Object missing, int size, int reverseMul, Weight weight,
                       ObjectMapper childObjectMapper, BitSetProducer parentFilter) {
        super(bigArrays, format, fieldType, missingBucket, missing, size, reverseMul, weight, childObjectMapper, parentFilter);
        this.breakerConsumer = breakerConsumer;
        this.docValuesFunc = docValuesFunc;
        this.values = bigArrays.newObjectArray(Math.min(size, 100));
        this.valueBuilders = bigArrays.newObjectArray(Math.min(size, 100));
    }

    @Override
    public void copyCurrent(int slot) {
        values =  bigArrays.grow(values, slot+1);
        valueBuilders = bigArrays.grow(valueBuilders, slot+1);
        BytesRefBuilder builder = valueBuilders.get(slot);
        int byteSize = builder == null ? 0 : builder.bytes().length;
        if (builder == null) {
            builder = new BytesRefBuilder();
            valueBuilders.set(slot, builder);
        }
        if (missingBucket && currentValue == null) {
            values.set(slot, null);
        } else {
            assert currentValue != null;
            builder.copyBytes(currentValue);
            breakerConsumer.accept(builder.bytes().length - byteSize);
            values.set(slot, builder.get());
        }
    }

    @Override
    public int compare(int from, int to) {
        if (missingBucket) {
            if (values.get(from) == null) {
                return values.get(to) == null ? 0 : -1 * reverseMul;
            } else if (values.get(to) == null) {
                return reverseMul;
            }
        }
        return compareValues(values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        if (missingBucket) {
            if (currentValue == null) {
                return values.get(slot) == null ? 0 : -1 * reverseMul;
            } else if (values.get(slot) == null) {
                return reverseMul;
            }
        }
        return compareValues(currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        if (missingBucket) {
            if (currentValue == null) {
                return afterValue == null ? 0 : -1 * reverseMul;
            } else if (afterValue == null) {
                return reverseMul;
            }
        }
        return compareValues(currentValue, afterValue);
    }

    int compareValues(BytesRef v1, BytesRef v2) {
        return v1.compareTo(v2) * reverseMul;
    }

    @Override
    void setAfter(Comparable<?> value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else if (value.getClass() == String.class) {
            afterValue = format.parseBytesRef(value.toString());
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) {
        return values.get(slot);
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedBinaryDocValues dvs = docValuesFunc.apply(context);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        currentValue = dvs.nextValue();
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValue = null;
                    next.collect(doc, bucket);
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable<?> value, LeafReaderContext context, LeafBucketCollector next) {
        if (value.getClass() != BytesRef.class) {
            throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
        }
        currentValue = (BytesRef) value;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                next.collect(doc, bucket);
            }
        };
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false ||
                fieldType instanceof StringFieldType == false ||
                    (query != null && query.getClass() != MatchAllDocsQuery.class)) {
            return null;
        }
        return new TermsSortedDocsProducer(fieldType.name());
    }

    @Override
    public void close() { Releasables.close(values, valueBuilders); }
}
