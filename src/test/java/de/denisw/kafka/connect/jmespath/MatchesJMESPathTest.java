package de.denisw.kafka.connect.jmespath;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchesJMESPathTest {

    private static final Schema USER_SCHEMA2 = SchemaBuilder
            .struct()
            .name("User")
            .field("email", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("BOD", Timestamp.SCHEMA)
            .build();

    private static final Struct EXAMPLE_USER2 = new Struct(USER_SCHEMA2)
            .put("email", "alice@example.com")
            .put("name", "Alice Example")
            .put("BOD",new Date(1698644746999L));

    private static final SinkRecord EXAMPLE_RECORD2 = new SinkRecord(
            "topic",
            0,
            null,
            null,
            USER_SCHEMA2,
            EXAMPLE_USER2,
            0);

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

    private static final SinkRecord EXAMPLE_RECORD = new SinkRecord(
            "topic",
            0,
            Schema.STRING_SCHEMA,
            "alice@example.com",
            USER_SCHEMA,
            EXAMPLE_USER,
            0);

//    @Test
//    void matchingKey() {
//        MatchesJMESPath.Key<SinkRecord> predicate =
//                new MatchesJMESPath.Key<>();
//
//        predicate.configure(Collections.singletonMap(
//                "query", "@ == 'alice@example.com'"));
//
//        assertTrue(predicate.test(EXAMPLE_RECORD));
//    }
//
//    @Test
//    void nonMatchingKey() {
//        MatchesJMESPath.Key<SinkRecord> predicate =
//                new MatchesJMESPath.Key<>();
//
//        predicate.configure(Collections.singletonMap(
//                "query", "@ == 'bob@example.com'"));
//
//        assertFalse(predicate.test(EXAMPLE_RECORD));
//    }

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

    @Test
    void MatchingValue2() {
        MatchesJMESPath.Value<SinkRecord> predicate =
                new MatchesJMESPath.Value<>();

        predicate.configure(Collections.singletonMap(
                "query", "BOD >= `1698644746999`"));

        assertTrue(predicate.test(EXAMPLE_RECORD2));
    }


}