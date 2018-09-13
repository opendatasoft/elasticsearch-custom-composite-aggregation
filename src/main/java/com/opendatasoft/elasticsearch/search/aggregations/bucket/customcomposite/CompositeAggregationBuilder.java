package com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CompositeAggregationBuilder extends AbstractAggregationBuilder<CompositeAggregationBuilder> {
    public static final String NAME = "custom_composite";

    public static final ParseField AFTER_FIELD_NAME = new ParseField("after");
    public static final ParseField SIZE_FIELD_NAME = new ParseField("size");
    public static final ParseField SOURCES_FIELD_NAME = new ParseField("sources");

    private static final ObjectParser<CompositeAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(NAME);
        PARSER.declareInt(CompositeAggregationBuilder::size, SIZE_FIELD_NAME);
        PARSER.declareObject(CompositeAggregationBuilder::aggregateAfter, (parser, context) -> parser.map(), AFTER_FIELD_NAME);
        PARSER.declareObjectArray(CompositeAggregationBuilder::setSources,
            (p, c) -> CompositeValuesSourceParserHelper.fromXContent(p), SOURCES_FIELD_NAME);
    }
    public static CompositeAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new CompositeAggregationBuilder(aggregationName), null);
    }

    private List<CompositeValuesSourceBuilder<?>> sources;
    private Map<String, Object> after;
    private int size = 10;

    private CompositeAggregationBuilder(String name) {
        this(name, null);
    }


    public CompositeAggregationBuilder(String name, List<CompositeValuesSourceBuilder<?>> sources) {
        super(name);
        this.sources = sources;
    }

    protected CompositeAggregationBuilder(CompositeAggregationBuilder clone,
                                          AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.sources = new ArrayList<>(clone.sources);
        this.after = clone.after;
        this.size = clone.size;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new CompositeAggregationBuilder(this, factoriesBuilder, metaData);
    }

    public CompositeAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        int num = in.readVInt();
        this.sources = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            CompositeValuesSourceBuilder<?> builder = CompositeValuesSourceParserHelper.readFrom(in);
            sources.add(builder);
        }
        this.size = in.readVInt();
        if (in.readBoolean()) {
            this.after = in.readMap();
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(sources.size());
        for (CompositeValuesSourceBuilder<?> builder : sources) {
            CompositeValuesSourceParserHelper.writeTo(builder, out);
        }
        out.writeVInt(size);
        out.writeBoolean(after != null);
        if (after != null) {
            out.writeMap(after);
        }
    }

    @Override
    public String getType() {
        return NAME;
    }

    private CompositeAggregationBuilder setSources(List<CompositeValuesSourceBuilder<?>> sources) {
        this.sources = sources;
        return this;
    }

    /**
     * Gets the list of {@link CompositeValuesSourceBuilder} for this aggregation.
     */
    public List<CompositeValuesSourceBuilder<?>> sources() {
        return sources;
    }

    /**
     * Sets the values that indicates which composite bucket this request should "aggregate after".
     * Defaults to <tt>null</tt>.
     */
    public CompositeAggregationBuilder aggregateAfter(Map<String, Object> afterKey) {
        this.after = afterKey;
        return this;
    }

    /**
     * The number of composite buckets to return. Defaults to <tt>10</tt>.
     */
    public CompositeAggregationBuilder size(int size) {
        this.size = size;
        return this;
    }

    @Override
    protected AggregatorFactory<?> doBuild(SearchContext context, AggregatorFactory<?> parent,
                                           AggregatorFactories.Builder subfactoriesBuilder) throws IOException {
        CompositeValuesSourceConfig[] configs = new CompositeValuesSourceConfig[sources.size()];
        for (int i = 0; i < configs.length; i++) {
            configs[i] = sources.get(i).build(context);
            if (configs[i].valuesSource().needsScores()) {
                throw new IllegalArgumentException("[sources] cannot access _score");
            }
        }
        final CompositeKey afterKey;
        if (after != null) {
            if (after.size() != configs.length) {
                throw new IllegalArgumentException("[after] has " + after.size() +
                    " value(s) but [sources] has " + sources.size());
            }
            Comparable<?>[] values = new Comparable<?>[sources.size()];
            for (int i = 0; i < sources.size(); i++) {
                String sourceName = sources.get(i).name();
                if (after.containsKey(sourceName) == false) {
                    throw new IllegalArgumentException("Missing value for [after." + sources.get(i).name() + "]");
                }
                Object obj = after.get(sourceName);
                if (obj instanceof Comparable) {
                    values[i] = (Comparable<?>) obj;
                } else {
                    throw new IllegalArgumentException("Invalid value for [after." + sources.get(i).name() +
                        "], expected comparable, got [" + (obj == null ? "null" :  obj.getClass().getSimpleName()) + "]");
                }
            }
            afterKey = new CompositeKey(values);
        } else {
            afterKey = null;
        }
        return new CompositeAggregationFactory(name, context, parent, subfactoriesBuilder, metaData, size, configs, afterKey);
    }


    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SIZE_FIELD_NAME.getPreferredName(), size);
        builder.startArray(SOURCES_FIELD_NAME.getPreferredName());
        for (CompositeValuesSourceBuilder<?> source: sources) {
            builder.startObject();
            builder.startObject(source.name());
            source.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
        }
        builder.endArray();
        if (after != null) {
            CompositeAggregation.buildCompositeMap(AFTER_FIELD_NAME.getPreferredName(), after, builder);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(sources, size, after);
    }

    @Override
    protected boolean doEquals(Object obj) {
        CompositeAggregationBuilder other = (CompositeAggregationBuilder) obj;
        return size == other.size &&
            Objects.equals(sources, other.sources) &&
            Objects.equals(after, other.after);
    }
}