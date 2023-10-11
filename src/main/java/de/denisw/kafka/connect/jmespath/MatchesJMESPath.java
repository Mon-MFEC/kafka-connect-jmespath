package de.denisw.kafka.connect.jmespath;

import io.burt.jmespath.Expression;
import io.burt.jmespath.node.NegateNode;
import io.burt.jmespath.parser.ParseException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.math.BigDecimal;
import java.util.Map;

/**
 * A {@link Predicate Kafka Connect predicate} which applies a JMESPath
 * query to the record key or value, and returns true if the query
 * evaluates to a JSON value that is "truthy" (every value other then
 * null, false, or an empty string, array or object).
 *
 * @see Key
 * @see Value
 * @see <a href="https://jmespath.org/">JMESPath</a>
 */
public abstract class MatchesJMESPath<R extends ConnectRecord<R>> implements Predicate<R> {

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(
            "query",
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            "The JMESPath query to evaluate for each record.");

    private final ConnectJMESPathRuntime runtime = new ConnectJMESPathRuntime();
    private Expression<Object> expression;
    private final String debeziumSchemaChange = "SchemaChangeValue";
    public static final String OVERVIEW_DOC = "Detect the byte fields and copy it to the new fields.";
    private static final String PURPOSE = OVERVIEW_DOC;
    private final Logger logger = LoggerFactory.getLogger(MatchesJMESPath.class);


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String query = (String) configs.get("query");
        try {
            expression = runtime.compile(query);
        } catch (ParseException e) {
            throw new ConfigException("query", query, e.getMessage());
        }
    }

    @Override
    public boolean test(R record) {
        if (dataToMatch(record) == null || dataToMatchSchema(record) == null) {
            return false;
        }

        Struct value = requireStruct(dataToMatch(record), PURPOSE);

        Schema valueSchema = value.schema();
        String valueSchemaName = valueSchema.name();
        if (valueSchemaName != null && valueSchemaName.contains(debeziumSchemaChange)){
            return false;
        }

        Object result = expression.search(dataToMatch(record));
        if (result instanceof NegateNode){
            logger.warn("monnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
        }

        if (expression instanceof NegateNode){
            logger.warn("piaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        }
        return runtime.isTruthy(result);
    }

    @Override
    public void close() {
        // Nothing to do
    }

    public static Struct requireStruct(Object value, String purpose) {
        if (!(value instanceof Struct)) {
            throw new DataException("Only Struct objects supported for [" + purpose + "], found: " + nullSafeClassName(value));
        }
        return (Struct) value;
    }

    private static String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getName();
    }

    protected abstract Object dataToMatch(R record);
    protected abstract Object dataToMatchSchema(R record);

    /**
     * A {@link MatchesJMESPath} predicate that applies the query to
     * the record's key.
     */
    public static class Key<R extends ConnectRecord<R>> extends MatchesJMESPath<R> {
        @Override
        protected Object dataToMatch(R record) {
            return record.key();
        }
        @Override
        protected Object dataToMatchSchema(R record) {
            return record.keySchema();
        }
    }

    /**
     * A {@link MatchesJMESPath} predicate that applies the query to
     * the record's value.
     */
    public static class Value<R extends ConnectRecord<R>> extends MatchesJMESPath<R> {
        @Override
        protected Object dataToMatch(R record) {
            return record.value();
        }
        @Override
        protected Object dataToMatchSchema(R record) {
            return record.valueSchema();
        }
    }
}
