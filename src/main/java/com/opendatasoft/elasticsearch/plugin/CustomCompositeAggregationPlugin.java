package com.opendatasoft.elasticsearch.plugin;

import com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite.CompositeAggregationBuilder;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite.InternalComposite;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;

public class CustomCompositeAggregationPlugin extends Plugin implements SearchPlugin {
    @Override
    public ArrayList<SearchPlugin.AggregationSpec> getAggregations() {
        ArrayList<SearchPlugin.AggregationSpec> r = new ArrayList<>();

        r.add(
                new AggregationSpec(
                        CompositeAggregationBuilder.NAME,
                        CompositeAggregationBuilder::new,
                        CompositeAggregationBuilder::parse)
                .addResultReader(InternalComposite::new)
        );

        return r;
    }
}
