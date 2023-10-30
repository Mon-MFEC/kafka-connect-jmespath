package de.denisw.kafka.connect.jmespath;

import java.math.BigDecimal;
import java.util.*;

import io.burt.jmespath.antlr.v4.runtime.tree.ParseTree;
import io.burt.jmespath.util.StringEscapeHelper;
import io.burt.jmespath.util.AntlrHelper;
import io.burt.jmespath.parser.ParseErrorAccumulator;
import io.burt.jmespath.parser.ParseException;
import io.burt.jmespath.parser.JmesPathBaseVisitor;
import io.burt.jmespath.parser.JmesPathParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonParserForBigNumeric extends JmesPathBaseVisitor<Object> {
    private static final StringEscapeHelper jsonEscapeHelper = new StringEscapeHelper(
            true,
            '"', '"',
            '/', '/',
            '\\', '\\',
            'b', '\b',
            'f', '\f',
            'n', '\n',
            'r', '\r',
            't', '\t'
    );

    private final Logger logger = LoggerFactory.getLogger(JsonParserForBigNumeric.class);
    private final ParseTree tree;
    private final ConnectJMESPathRuntime runtime;

    public static Object fromString(String json, ConnectJMESPathRuntime runtime) {
        ParseErrorAccumulator errors = new ParseErrorAccumulator();
        JmesPathParser parser = AntlrHelper.createParser(json, errors);
        ParseTree tree = parser.jsonValue();
        if (errors.isEmpty()) {
            return new JsonParserForBigNumeric(tree, runtime).object();
        } else {
            throw new ParseException(json, errors);
        }
    }

    private JsonParserForBigNumeric(ParseTree tree, ConnectJMESPathRuntime runtime) {
        this.tree = tree;
        this.runtime = runtime;
    }

    public Object object() {
        return visit(tree);
    }

    private String unquote(String quotedString) {
        return quotedString.substring(1, quotedString.length() - 1);
    }

    @Override
    public Object visitJsonObject(JmesPathParser.JsonObjectContext ctx) {
        Map<Object, Object> object = new LinkedHashMap<>(ctx.jsonObjectPair().size());
        for (final JmesPathParser.JsonObjectPairContext pair : ctx.jsonObjectPair()) {
            String key = jsonEscapeHelper.unescape(unquote(pair.STRING().getText()));
            Object value = visit(pair.jsonValue());
            object.put(key, value);
        }
        return runtime.createObject(object);
    }

    @Override
    public Object visitJsonArray(JmesPathParser.JsonArrayContext ctx) {
        List<Object> array = new ArrayList<>(ctx.jsonValue().size());
        for (final JmesPathParser.JsonValueContext entry : ctx.jsonValue()) {
            array.add(visit(entry));
        }
        return runtime.createArray(array);
    }

    @Override
    public Object visitJsonStringValue(JmesPathParser.JsonStringValueContext ctx) {
        return runtime.createString(jsonEscapeHelper.unescape(unquote(ctx.getText())));
    }

    @Override
    public Object visitJsonNumberValue(JmesPathParser.JsonNumberValueContext ctx) {
        return runtime.createNumber(new BigDecimal(ctx.getText()));
    }

    @Override
    public Object visitJsonObjectValue(JmesPathParser.JsonObjectValueContext ctx) {
        logger.warn("Visit Object");
        if (visit(ctx.jsonObject()) instanceof Date || visit(ctx.jsonObject()) instanceof java.sql.Date)
            logger.warn("Instance of Date ");
        return visit(ctx.jsonObject());
    }

    @Override
    public Object visitJsonArrayValue(JmesPathParser.JsonArrayValueContext ctx) {
        return visit(ctx.jsonArray());
    }

    @Override
    public Object visitJsonConstantValue(JmesPathParser.JsonConstantValueContext ctx) {
        switch (ctx.getText().charAt(0)) {
            case 't': return runtime.createBoolean(true);
            case 'f': return runtime.createBoolean(false);
            case 'n': return runtime.createNull();
            default: throw new IllegalStateException(String.format("Expected true, false or null but encountered %s", ctx.getText()));
        }
    }
}
