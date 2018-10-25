package com.opendatasoft.elasticsearch.search.aggregations.bucket.customcomposite;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

class CompositeValuesSourceParserHelper {
    static <VB extends CompositeValuesSourceBuilder<VB>, T> void declareValuesSourceFields(AbstractObjectParser<VB, T> objectParser,
                                                                                           ValueType targetValueType) {
        objectParser.declareField(VB::field, XContentParser::text,
            new ParseField("field"), ObjectParser.ValueType.STRING);
        objectParser.declareField(VB::missing, XContentParser::objectText,
            new ParseField("missing"), ObjectParser.ValueType.VALUE);
        objectParser.declareBoolean(VB::missingBucket, new ParseField("missing_bucket"));

        objectParser.declareField(VB::valueType, p -> {
            ValueType valueType = ValueType.resolveForScript(p.text());
            if (targetValueType != null && valueType.isNotA(targetValueType)) {
                throw new ParsingException(p.getTokenLocation(),
                    "Aggregation [" + objectParser.getName() + "] was configured with an incompatible value type ["
                        + valueType + "]. It can only work on value of type ["
                        + targetValueType + "]");
            }
            return valueType;
        }, new ParseField("value_type"), ObjectParser.ValueType.STRING);

        objectParser.declareField(VB::script,
            (parser, context) -> Script.parse(parser), Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);

        objectParser.declareField(VB::order,  XContentParser::text, new ParseField("order"), ObjectParser.ValueType.STRING);

        objectParser.declareField(
                VB::nested, (parser, context) -> CompositeValuesSourceParserHelper.parseNested(parser),
                new ParseField("nested"), ObjectParser.ValueType.OBJECT);
    }

    private static Tuple<String, QueryBuilder> parseNested(XContentParser parser) throws IOException {
        QueryBuilder filter = null;
        String path = null;
        String currentFieldName = null;
        ParseField pathField = new ParseField("path");
        ParseField filterField = new ParseField("filter");
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (pathField.match(currentFieldName, parser.getDeprecationHandler())) {
                    path = parser.text();
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (filterField.match(currentFieldName, parser.getDeprecationHandler())) {
                    filter = parseInnerQueryBuilder(parser);
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else {
                XContentParserUtils.throwUnknownToken(token, parser.getTokenLocation());
            }
        }
        return new Tuple<>(path, filter);
    }

    static void writeTo(CompositeValuesSourceBuilder<?> builder, StreamOutput out) throws IOException {
        final byte code;
        if (builder.getClass() == TermsValuesSourceBuilder.class) {
            code = 0;
        } else if (builder.getClass() == DateHistogramValuesSourceBuilder.class) {
            code = 1;
        } else if (builder.getClass() == HistogramValuesSourceBuilder.class) {
            code = 2;
        } else {
            throw new IOException("invalid builder type: " + builder.getClass().getSimpleName());
        }
        out.writeByte(code);
        builder.writeTo(out);
    }

    static CompositeValuesSourceBuilder<?> readFrom(StreamInput in) throws IOException {
        int code = in.readByte();
        switch(code) {
            case 0:
                return new TermsValuesSourceBuilder(in);
            case 1:
                return new DateHistogramValuesSourceBuilder(in);
            case 2:
                return new HistogramValuesSourceBuilder(in);
            default:
                throw new IOException("Invalid code " + code);
        }
    }

    static CompositeValuesSourceBuilder<?> fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
        String name = parser.currentName();
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
        String type = parser.currentName();
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        final CompositeValuesSourceBuilder<?> builder;
        switch(type) {
            case TermsValuesSourceBuilder.TYPE:
                builder = TermsValuesSourceBuilder.parse(name, parser);
                break;
            case DateHistogramValuesSourceBuilder.TYPE:
                builder = DateHistogramValuesSourceBuilder.parse(name, parser);
                break;
            case HistogramValuesSourceBuilder.TYPE:
                builder = HistogramValuesSourceBuilder.parse(name, parser);
                break;
            default:
                throw new ParsingException(parser.getTokenLocation(), "invalid source type: " + type);
        }
        parser.nextToken();
        parser.nextToken();
        return builder;
    }
}
