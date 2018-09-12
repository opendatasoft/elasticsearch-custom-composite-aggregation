package com.opendatasoft.elasticsearch.search.aggregations.bucket.composite_filter_nested;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class CompositeAggregationFactory extends AggregatorFactory<CompositeAggregationFactory> {
    private final int size;
    private final CompositeValuesSourceConfig[] sources;
    private final CompositeKey afterKey;

    CompositeAggregationFactory(String name, SearchContext context, AggregatorFactory<?> parent,
                                AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData,
                                int size, CompositeValuesSourceConfig[] sources, CompositeKey afterKey) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metaData);
        this.size = size;
        this.sources = sources;
        this.afterKey = afterKey;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket,
                                        List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        return new CompositeAggregator(name, factories, context, parent, pipelineAggregators, metaData,
            size, sources, afterKey);
    }
}
