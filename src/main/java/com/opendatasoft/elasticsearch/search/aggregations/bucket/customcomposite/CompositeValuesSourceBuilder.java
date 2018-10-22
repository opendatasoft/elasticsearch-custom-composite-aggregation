package com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link ValuesSource} builder for {@link CompositeAggregationBuilder}
 */
public abstract class CompositeValuesSourceBuilder<AB extends CompositeValuesSourceBuilder<AB>> implements Writeable, ToXContentFragment {
    protected final String name;
    private String field = null;
    private Script script = null;
    private ValueType valueType = null;
    private boolean missingBucket = false;
    private Object missing = null;
    private SortOrder order = SortOrder.ASC;
    private String format = null;
    private QueryBuilder filter = null;
    private String nestedPath = null;

    CompositeValuesSourceBuilder(String name) {
        this(name, null);
    }

    CompositeValuesSourceBuilder(String name, ValueType valueType) {
        this.name = name;
        this.valueType = valueType;
    }

    CompositeValuesSourceBuilder(StreamInput in) throws IOException {
        this.name = in.readString();
        this.field = in.readOptionalString();
        if (in.readBoolean()) {
            this.script = new Script(in);
        }
        if (in.readBoolean()) {
            this.valueType = ValueType.readFromStream(in);
        }
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            this.missingBucket = in.readBoolean();
        } else {
            this.missingBucket = false;
        }
        this.missing = in.readGenericValue();
        this.order = SortOrder.readFromStream(in);
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            this.format = in.readOptionalString();
        } else {
            this.format = null;
        }
        if (in.readBoolean()) {
            filter = in.readNamedWriteable(QueryBuilder.class);
        }
        this.nestedPath = in.readOptionalString();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(field);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        boolean hasValueType = valueType != null;
        out.writeBoolean(hasValueType);
        if (hasValueType) {
            valueType.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeBoolean(missingBucket);
        }
        out.writeGenericValue(missing);
        order.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeOptionalString(format);
        }
        innerWriteTo(out);
        boolean hasFilter = filter != null;
        out.writeBoolean(hasFilter);
        if (hasFilter) {
            out.writeNamedWriteable(filter);
        }
        out.writeOptionalString(nestedPath);
    }

    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    protected abstract void doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(type());
        if (field != null) {
            builder.field("field", field);
        }
        if (script != null) {
            builder.field("script", script);
        }
        builder.field("missing_bucket", missingBucket);
        if (missing != null) {
            builder.field("missing", missing);
        }
        if (valueType != null) {
            builder.field("value_type", valueType.getPreferredName());
        }
        if (format != null) {
            builder.field("format", format);
        }
        if (filter != null) {
            filter.toXContent(builder, params);
        }
        if (nestedPath != null) {
            builder.field("nested_path", nestedPath);
        }
        builder.field("order", order);
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(field, missingBucket, missing, script, valueType, order, format, filter, nestedPath, innerHashCode());
    }

    protected abstract int innerHashCode();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        @SuppressWarnings("unchecked")
        AB that = (AB) o;
        return Objects.equals(field, that.field()) &&
            Objects.equals(script, that.script()) &&
            Objects.equals(valueType, that.valueType()) &&
            Objects.equals(missingBucket, that.missingBucket()) &&
            Objects.equals(missing, that.missing()) &&
            Objects.equals(order, that.order()) &&
            Objects.equals(format, that.format()) &&
            Objects.equals(filter, that.filter()) &&
            Objects.equals(nestedPath, that.nestedPath()) &&
            innerEquals(that);
    }

    protected abstract boolean innerEquals(AB builder);

    public String name() {
        return name;
    }

    abstract String type();

    /**
     * Sets the field to use for this source
     */
    @SuppressWarnings("unchecked")
    public AB field(String field) {
        if (field == null) {
            throw new IllegalArgumentException("[field] must not be null");
        }
        this.field = field;
        return (AB) this;
    }

    /**
     * Gets the field to use for this source
     */
    public String field() {
        return field;
    }

    /**
     * Sets the filter to use for this source
     */
    @SuppressWarnings("unchecked")
    public AB filter(QueryBuilder filter) {
        if (filter == null) {
            throw new IllegalArgumentException("[filter] must not be null");
        }
        this.filter = filter;
        return (AB) this;
    }

    /**
     * Gets the field to use for this source
     */
    public QueryBuilder filter() {
        return filter;
    }

    /**
     * Sets the filter to use for this source
     */
    @SuppressWarnings("unchecked")
    public AB nestedPath(String nestedPath) {
        if (nestedPath == null) {
            throw new IllegalArgumentException("[nested_path] must not be null");
        }
        this.nestedPath = nestedPath;
        return (AB) this;
    }

    /**
     * Gets the field to use for this source
     */
    public String nestedPath() {
        return nestedPath;
    }

    /**
     * Sets the script to use for this source
     */
    @SuppressWarnings("unchecked")
    public AB script(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("[script] must not be null");
        }
        this.script = script;
        return (AB) this;
    }

    /**
     * Gets the script to use for this source
     */
    public Script script() {
        return script;
    }

    /**
     * Sets the {@link ValueType} for the value produced by this source
     */
    @SuppressWarnings("unchecked")
    public AB valueType(ValueType valueType) {
        if (valueType == null) {
            throw new IllegalArgumentException("[valueType] must not be null");
        }
        this.valueType = valueType;
        return (AB) this;
    }

    /**
     * Gets the {@link ValueType} for the value produced by this source
     */
    public ValueType valueType() {
        return valueType;
    }

    /**
     * Sets the value to use when the source finds a missing value in a
     * document
     */
    @SuppressWarnings("unchecked")
    public AB missing(Object missing) {
        if (missing == null) {
            throw new IllegalArgumentException("[missing] must not be null");
        }
        this.missing = missing;
        return (AB) this;
    }

    public Object missing() {
        return missing;
    }

    /**
     * If true an explicit `null bucket will represent documents with missing values.
     */
    @SuppressWarnings("unchecked")
    public AB missingBucket(boolean missingBucket) {
        this.missingBucket = missingBucket;
        return (AB) this;
    }

    /**
     * False if documents with missing values are ignored, otherwise missing values are
     * represented by an explicit `null` value.
     */
    public boolean missingBucket() {
        return missingBucket;
    }

    /**
     * Sets the {@link SortOrder} to use to sort values produced this source
     */
    @SuppressWarnings("unchecked")
    public AB order(String order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null");
        }
        this.order = SortOrder.fromString(order);
        return (AB) this;
    }


    /**
     * Sets the {@link SortOrder} to use to sort values produced this source
     */
    @SuppressWarnings("unchecked")
    public AB order(SortOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null");
        }
        this.order = order;
        return (AB) this;
    }

    /**
     * Gets the {@link SortOrder} to use to sort values produced this source
     */
    public SortOrder order() {
        return order;
    }

    /**
     * Sets the format to use for the output of the aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return (AB) this;
    }

    /**
     * Gets the format to use for the output of the aggregation.
     */
    public String format() {
        return format;
    }

    /**
     * Creates a {@link CompositeValuesSourceConfig} for this source.
     *
     * @param context   The search context for this source.
     * @param config    The {@link ValuesSourceConfig} for this source.
     */
    protected abstract CompositeValuesSourceConfig innerBuild(SearchContext context, ValuesSourceConfig<?> config) throws IOException;

    public final CompositeValuesSourceConfig build(SearchContext context) throws IOException {
        ValuesSourceConfig<?> config = ValuesSourceConfig.resolve(context.getQueryShardContext(),
            valueType, field, script, missing, null, format);

        if (config.unmapped() && field != null && missing == null && missingBucket == false) {
            // this source cannot produce any values so we refuse to build
            // since composite buckets are not created on null values
            throw new QueryShardException(context.getQueryShardContext(),
                "failed to find field [" + field + "] and [missing] is not provided");
        }
        if (missingBucket && missing != null) {
            throw new QueryShardException(context.getQueryShardContext(),
                    "cannot use [missing] option in conjunction with [missing_bucket]");
        }
        return innerBuild(context, config);
    }
}
