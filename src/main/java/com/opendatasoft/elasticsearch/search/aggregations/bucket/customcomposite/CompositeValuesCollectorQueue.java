package com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeMap;

/**
 * A specialized queue implementation for composite buckets
 */
final class CompositeValuesCollectorQueue implements Releasable {
    // the slot for the current candidate
    private static final int CANDIDATE_SLOT = Integer.MAX_VALUE;

    private final int maxSize;
    private final TreeMap<Integer, Integer> keys;
    private final SingleDimensionValuesSource<?>[] arrays;
    private final int[] docCounts;
    private boolean afterValueSet = false;

    /**
     * Constructs a composite queue with the specified size and sources.
     *
     * @param sources The list of {@link CompositeValuesSourceConfig} to build the composite buckets.
     * @param size The number of composite buckets to keep.
     */
    CompositeValuesCollectorQueue(SingleDimensionValuesSource<?>[] sources, int size) {
        this.maxSize = size;
        this.arrays = sources;
        this.docCounts = new int[size];
        this.keys = new TreeMap<>(this::compare);
    }

    void clear() {
        keys.clear();
        Arrays.fill(docCounts, 0);
        afterValueSet = false;
    }

    /**
     * The current size of the queue.
     */
    int size() {
        return keys.size();
    }

    /**
     * Whether the queue is full or not.
     */
    boolean isFull() {
        return keys.size() == maxSize;
    }

    /**
     * Returns a sorted {@link Set} view of the slots contained in this queue.
     */
    Set<Integer> getSortedSlot() {
        return keys.keySet();
    }

    /**
     * Compares the current candidate with the values in the queue and returns
     * the slot if the candidate is already in the queue or null if the candidate is not present.
     */
    Integer compareCurrent() {
        return keys.get(CANDIDATE_SLOT);
    }

    /**
     * Returns the lowest value (exclusive) of the leading source.
     */
    Comparable<?> getLowerValueLeadSource() {
        return afterValueSet ? arrays[0].getAfter() : null;
    }

    /**
     * Returns the upper value (inclusive) of the leading source.
     */
    Comparable<?> getUpperValueLeadSource() throws IOException {
        return size() >= maxSize ? arrays[0].toComparable(keys.lastKey()) : null;
    }
    /**
     * Returns the document count in <code>slot</code>.
     */
    int getDocCount(int slot) {
        return docCounts[slot];
    }

    /**
     * Copies the current value in <code>slot</code>.
     */
    private void copyCurrent(int slot) {
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].copyCurrent(slot);
        }
        docCounts[slot] = 1;
    }

    /**
     * Compares the values in <code>slot1</code> with <code>slot2</code>.
     */
    int compare(int slot1, int slot2) {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = (slot1 == CANDIDATE_SLOT) ? arrays[i].compareCurrent(slot2) :
                arrays[i].compare(slot1, slot2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Sets the after values for this comparator.
     */
    void setAfter(Comparable<?>[] values) {
        assert values.length == arrays.length;
        afterValueSet = true;
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].setAfter(values[i]);
        }
    }

    /**
     * Compares the after values with the values in <code>slot</code>.
     */
    private int compareCurrentWithAfter() {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = arrays[i].compareCurrentWithAfter();
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Builds the {@link CompositeKey} for <code>slot</code>.
     */
    CompositeKey toCompositeKey(int slot) throws IOException {
        assert slot < maxSize;
        Comparable<?>[] values = new Comparable<?>[arrays.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = arrays[i].toComparable(slot);
        }
        return new CompositeKey(values);
    }

    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector in) throws IOException {
        return getLeafCollector(null, context, in);
    }
    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * If <code>forceLeadSourceValue</code> is not null, the leading source will use this value
     * for each document.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(Comparable<?> forceLeadSourceValue,
                                         LeafReaderContext context, LeafBucketCollector in) throws IOException {

        InternalCollector internalCollectors[] = new InternalCollector[arrays.length];

        LeafBucketCollector empty = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {

            }
        };

        for(int i = arrays.length - 1; i >= 0; i--) {
            if (i == 0) {
                internalCollectors[i] = new InternalCollector(arrays[i], context, empty, forceLeadSourceValue);
            } else {
                internalCollectors[i] = new InternalCollector(arrays[i], context, empty, null);
            }
        }

        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {

                for (int i=0; i < internalCollectors.length; i++) {
                    if (internalCollectors[i].nested) {
                        if (doc == 0 || internalCollectors[i].parentDocs == null || internalCollectors[i].childDocs == null) {
                            return;
                        }

                        final int prevParentDoc = internalCollectors[i].parentDocs.prevSetBit(doc - 1);
                        int childDocId = internalCollectors[i].childDocs.docID();
                        if (childDocId <= prevParentDoc) {
                            childDocId = internalCollectors[i].childDocs.advance(prevParentDoc + 1);
                        }

                        for (; childDocId < doc; childDocId = internalCollectors[i].childDocs.nextDoc()) {
                            if (internalCollectors[i].filter == null || internalCollectors[i].filter.get(childDocId)) {
                                internalCollectors[i].next.collect(childDocId, bucket);
                            }
                        }
                    }
                    else {
                        if (internalCollectors[i].filter == null || internalCollectors[i].filter.get(doc)) {
                            internalCollectors[i].next.collect(doc, bucket);
                        }
                    }
                }
                in.collect(doc, bucket);

            }
        };
    }

    public static class InternalCollector {
        BitSet parentDocs;
        DocIdSetIterator childDocs;
        boolean nested = false;
        LeafBucketCollector next;
        Bits filter = null;

        InternalCollector(
                SingleDimensionValuesSource<?> singleDimensionValuesSource, LeafReaderContext context,
                LeafBucketCollector next, Comparable<?> forceLeadSourceValue) throws IOException {

            if (forceLeadSourceValue != null) {
                this.next = singleDimensionValuesSource.getLeafCollector(forceLeadSourceValue, context, next);
            } else {
                this.next = singleDimensionValuesSource.getLeafCollector(context, next);
            }
            if (singleDimensionValuesSource.childObjectMapper != null) {
                nested = true;
                Query childFilter = singleDimensionValuesSource.childObjectMapper.nestedTypeFilter();
                IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
                IndexSearcher searcher = new IndexSearcher(topLevelContext);
                searcher.setQueryCache(null);
                Weight weight = searcher.createNormalizedWeight(childFilter, false);
                Scorer childDocsScorer = weight.scorer(context);
                parentDocs = singleDimensionValuesSource.parentFilter.getBitSet(context);
                childDocs = childDocsScorer != null ? childDocsScorer.iterator() : null;
            }
            if (singleDimensionValuesSource.weight != null) {
                filter = Lucene.asSequentialAccessBits(
                        context.reader().maxDoc(), singleDimensionValuesSource.weight.scorerSupplier(context));
            }
        }
    }

    /**
     * Check if the current candidate should be added in the queue.
     * @return The target slot of the candidate or -1 is the candidate is not competitive.
     */
    int addIfCompetitive() {
        // checks if the candidate key is competitive
        Integer topSlot = compareCurrent();

        if (topSlot != null) {
            // this key is already in the top N, skip it
            docCounts[topSlot] += 1;
            return topSlot;
        }
        if (afterValueSet && compareCurrentWithAfter() <= 0) {
            // this key is greater than the top value collected in the previous round, skip it
            return -1;
        }
        if (keys.size() >= maxSize) {
            // the tree map is full, check if the candidate key should be kept
            if (compare(CANDIDATE_SLOT, keys.lastKey()) > 0) {
                // the candidate key is not competitive, skip it
                return -1;
            }
        }

        // the candidate key is competitive
        final int newSlot;
        if (keys.size() >= maxSize) {
            // the tree map is full, we replace the last key with this candidate
            int slot = keys.pollLastEntry().getKey();
            // and we recycle the deleted slot
            newSlot = slot;
        } else {
            newSlot = keys.size();
            assert newSlot < maxSize;
        }
        // move the candidate key to its new slot
        copyCurrent(newSlot);
        keys.put(newSlot, newSlot);
        return newSlot;
    }


    @Override
    public void close() {
        Releasables.close(arrays);
    }
}