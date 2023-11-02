package de.denisw.kafka.connect.jmespath;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchesJMESPathTest {
    private static final Schema ADDRESS_SCHEMA = SchemaBuilder
            .struct()
            .name("Address")
            .field("street", Schema.STRING_SCHEMA)
            .field("city", Schema.STRING_SCHEMA)
            .field("postalCode", Decimal.schema(0))
            .field("country", Schema.STRING_SCHEMA)
            .build();

    private static final Schema USER_SCHEMA = SchemaBuilder
            .struct()
            .name("User")
            .field("email", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("address", ADDRESS_SCHEMA)
            .build();

    private static final Struct EXAMPLE_USER = new Struct(USER_SCHEMA)
            .put("email", "alice@example.com")
            .put("name", "Alice Example")
            .put("address", new Struct(ADDRESS_SCHEMA)
                    .put("street", "Musterstr. 123")
                    .put("postalCode", new BigDecimal("123456789123456789123456789123456789.123456789123456789"))
                    .put("city", "Berlin")
                    .put("country", "DE"));

    private static final Schema USER_SCHEMA_KEY = SchemaBuilder
            .struct()
            .name("User's key")
            .field("email", Schema.STRING_SCHEMA)
            .build();

    private static final Struct EXAMPLE_USER_KEY = new Struct(USER_SCHEMA_KEY)
            .put("email", "alice@example.com");

    private static final SinkRecord EXAMPLE_RECORD = new SinkRecord(
            "topic",
            0,
            USER_SCHEMA_KEY,
            EXAMPLE_USER_KEY,
            USER_SCHEMA,
            EXAMPLE_USER,
            0);

    @Test
    void matchingKey() {
        MatchesJMESPath.Key<SinkRecord> predicate =
                new MatchesJMESPath.Key<>();

        predicate.configure(Collections.singletonMap(
                "query", "email == 'alice@example.com'"));

        assertTrue(predicate.test(EXAMPLE_RECORD));
    }

    @Test
    void nonMatchingKey() {
        MatchesJMESPath.Key<SinkRecord> predicate =
                new MatchesJMESPath.Key<>();

        predicate.configure(Collections.singletonMap(
                "query", "email == 'bob@example.com'"));

        assertFalse(predicate.test(EXAMPLE_RECORD));
    }

    @Test
    void nonMatchingValue() {
        MatchesJMESPath.Value<SinkRecord> predicate =
                new MatchesJMESPath.Value<>();

        predicate.configure(Collections.singletonMap(
                "query", "address.city == 'New York'"));

        assertFalse(predicate.test(EXAMPLE_RECORD));
    }

    @Test
    void MatchingValue() {
        MatchesJMESPath.Value<SinkRecord> predicate =
                new MatchesJMESPath.Value<>();

        predicate.configure(Collections.singletonMap(
                "query", "address.postalCode >= `123456789123456789123456789123456789.123456789123456788`"));

        assertTrue(predicate.test(EXAMPLE_RECORD));
    }
}