package de.denisw.kafka.connect.jmespath;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchesJMESPathTest {

    private static final Schema ADDRESS_SCHEMA = SchemaBuilder
            .struct()
            .name("Address")
            .field("street", Schema.STRING_SCHEMA)
            .field("city", Schema.STRING_SCHEMA)
            .field("postalCode", Schema.STRING_SCHEMA)
            .field("country", Schema.STRING_SCHEMA)
            .build();

    private static final Schema USER_SCHEMA = SchemaBuilder
            .struct()
            .name("User")
            .field("email", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("address", ADDRESS_SCHEMA)
            .build();

    private static final Schema KEY_SCHEMA = SchemaBuilder
            .struct()
            .name("KEY_User")
            .field("email", Schema.STRING_SCHEMA);

    private static final Struct EXAMPLE_KEYUSER = new Struct(KEY_SCHEMA)
            .put("email", "alice@example.com");

    private static final Struct EXAMPLE_USER = new Struct(USER_SCHEMA)
            .put("email", "alice@example.com")
            .put("name", "Alice Example")
            .put("address", new Struct(ADDRESS_SCHEMA)
                    .put("street", "Musterstr. 123")
                    .put("postalCode", "12345")
                    .put("city", "Berlin")
                    .put("country", "DE"));

    private static final SinkRecord EXAMPLE_RECORD2 = new SinkRecord(
            "topic",
            0,
            KEY_SCHEMA,
            EXAMPLE_KEYUSER,
            USER_SCHEMA,
            null,
            0);

    private static final SinkRecord EXAMPLE_RECORD3 = new SinkRecord(
            "topic",
            0,
            KEY_SCHEMA,
            EXAMPLE_KEYUSER,
            null,
            EXAMPLE_USER,
            0);



    private static final SinkRecord EXAMPLE_RECORD = new SinkRecord(
            "topic",
            0,
            KEY_SCHEMA,
            EXAMPLE_KEYUSER,
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
    void nonMatchingValue2() {
        MatchesJMESPath.Value<SinkRecord> predicate =
                new MatchesJMESPath.Value<>();

        predicate.configure(Collections.singletonMap(
                "query", "address.city == 'New York'"));

        assertFalse(predicate.test(EXAMPLE_RECORD2));
    }


    @Test
    void nonMatchingValue3() {
        MatchesJMESPath.Value<SinkRecord> predicate =
                new MatchesJMESPath.Value<>();

        predicate.configure(Collections.singletonMap(
                "query", "address.city == 'New York'"));

        assertFalse(predicate.test(EXAMPLE_RECORD3));
    }





}