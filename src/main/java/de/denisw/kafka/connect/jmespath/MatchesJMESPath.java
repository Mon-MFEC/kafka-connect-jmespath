package de.denisw.kafka.connect.jmespath;

import io.burt.jmespath.Expression;
import io.burt.jmespath.parser.ParseException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.predicates.Predicate;

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
    public static final String OVERVIEW_DOC = "Custom predicate with value.";
    private static final String PURPOSE = OVERVIEW_DOC;

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(
            "query",
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            "The JMESPath query to evaluate for each record.");

    private final String debeziumSchemaChange = "SchemaChangeValue";
    private final ConnectJMESPathRuntime runtime = new ConnectJMESPathRuntime();
    private Expression<Object> expression;

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
        Object valueObject = operatingValue(record);
        Schema valueSchema = operatingSchema(record);
        if (valueObject == null || valueSchema == null)
            return false;

        Struct value = requireStruct(operatingValue(record), PURPOSE);
        String valueSchemaName = valueSchema.name();
        if (valueSchemaName != null && valueSchemaName.contains(debeziumSchemaChange))
            return false;

        Object result = expression.search(value);
        return runtime.isTruthy(result);
    }

    private Struct requireStruct(Object value, String purpose) {
        if (!(value instanceof Struct)) {
            throw new DataException("Only Struct objects supported for [" + purpose + "], found: " + nullSafeClassName(value));
        }
        return (Struct) value;
    }

    private String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getName();
    }

    @Override
    public void close() {
        // Nothing to do
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    /**
     * A {@link MatchesJMESPath} predicate that applies the query to
     * the record's key.
     */
    public static class Key<R extends ConnectRecord<R>> extends MatchesJMESPath<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
    }

    /**
     * A {@link MatchesJMESPath} predicate that applies the query to
     * the record's value.
     */
    public static class Value<R extends ConnectRecord<R>> extends MatchesJMESPath<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
    }
}
