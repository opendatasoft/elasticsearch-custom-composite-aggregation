package com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * A source that can record and compare values of similar type.
 */
abstract class SingleDimensionValuesSource<T extends Comparable<T>> implements Releasable {
    protected final BigArrays bigArrays;
    protected final DocValueFormat format;
    @Nullable
    protected final MappedFieldType fieldType;
    @Nullable
    protected final Object missing;
    protected final boolean missingBucket;

    protected final int size;
    protected final int reverseMul;

    protected T afterValue;

    protected Weight weight;

    protected ObjectMapper childObjectMapper;

    protected BitSetProducer parentFilter;

    /**
     * Creates a new {@link SingleDimensionValuesSource}.
     *
     * @param format The format of the source.
     * @param fieldType The field type or null if the source is a script.
     * @param missing The missing value or null if documents with missing value should be ignored.
     * @param missingBucket If true, an explicit `null bucket represents documents with missing values.
     * @param size The number of values to record.
     * @param reverseMul -1 if the natural order ({@link SortOrder#ASC} should be reversed.
     */
    SingleDimensionValuesSource(BigArrays bigArrays, DocValueFormat format, @Nullable MappedFieldType fieldType,
                                boolean missingBucket, @Nullable Object missing,
                                int size, int reverseMul, Weight weight, ObjectMapper childObjectMapper,
                                BitSetProducer parentFilter) {
        assert missing == null || missingBucket == false;
        this.bigArrays = bigArrays;
        this.format = format;
        this.fieldType = fieldType;
        this.missing = missing;
        this.missingBucket = missingBucket;
        this.size = size;
        this.reverseMul = reverseMul;
        this.afterValue = null;
        this.weight = weight;
        this.childObjectMapper = childObjectMapper;
        this.parentFilter = parentFilter;
    }

    /**
     * The current value is filled by a {@link LeafBucketCollector} that visits all the
     * values of each document. This method saves this current value in a slot and should only be used
     * in the context of a collection.
     * See {@link this#getLeafCollector}.
     */
    abstract void copyCurrent(int slot);

    /**
     * Compares the value in <code>from</code> with the value in <code>to</code>.
     */
    abstract int compare(int from, int to);

    /**
     * The current value is filled by a {@link LeafBucketCollector} that visits all the
     * values of each document. This method compares this current value with the value present in
     * the provided slot and should only be used in the context of a collection.
     * See {@link this#getLeafCollector}.
     */
    abstract int compareCurrent(int slot);

    /**
     * The current value is filled by a {@link LeafBucketCollector} that visits all the
     * values of each document. This method compares this current value with the after value
     * set on this source and should only be used in the context of a collection.
     * See {@link this#getLeafCollector}.
     */
    abstract int compareCurrentWithAfter();

    /**
     * Sets the after value for this source. Values that compares smaller are filtered.
     */
    abstract void setAfter(Comparable<?> value);

    /**
     * Returns the after value set for this source.
     */
    T getAfter() {
        return afterValue;
    }

    /**
     * Transforms the value in <code>slot</code> to a {@link Comparable} object.
     */
    abstract T toComparable(int slot) throws IOException;

    /**
     * Creates a {@link LeafBucketCollector} that extracts all values from a document and invokes
     * {@link LeafBucketCollector#collect} on the provided <code>next</code> collector for each of them.
     * The current value of this source is set on each call and can be accessed by <code>next</code> via
     * the {@link this#copyCurrent(int)} and {@link this#compareCurrent(int)} methods. Note that these methods
     * are only valid when invoked from the {@link LeafBucketCollector} created in this source.
     */
    abstract LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException;

    /**
     * Creates a {@link LeafBucketCollector} that sets the current value for each document to the provided
     * <code>value</code> and invokes {@link LeafBucketCollector#collect} on the provided <code>next</code> collector.
     */
    abstract LeafBucketCollector getLeafCollector(Comparable<?> value,
                                                  LeafReaderContext context, LeafBucketCollector next) throws IOException;

    /**
     * Returns a {@link SortedDocsProducer} or null if this source cannot produce sorted docs.
     */
    abstract SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query);

    /**
     * Returns true if a {@link SortedDocsProducer} should be used to optimize the execution.
     */
    protected boolean checkIfSortedDocsIsApplicable(IndexReader reader, MappedFieldType fieldType) {
        if (fieldType == null ||
                missing != null ||
                (missingBucket && afterValue == null) ||
                fieldType.indexOptions() == IndexOptions.NONE ||
                // inverse of the natural order
                reverseMul == -1) {
            return false;
        }

        if (reader.hasDeletions() &&
                (reader.numDocs() == 0 || (double) reader.numDocs() / (double) reader.maxDoc() < 0.5)) {
            // do not use the index if it has more than 50% of deleted docs
            return false;
        }
        return true;
    }
}