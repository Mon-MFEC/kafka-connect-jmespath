package de.denisw.kafka.connect.jmespath;

import io.burt.jmespath.JmesPathType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ConnectJMESPathRuntimeTest {

    private final ConnectJMESPathRuntime runtime = new ConnectJMESPathRuntime();

    @Test
    @SuppressWarnings("unchecked")
    void parseString() {
        String json = ""
                + "{"
                + "  \"boolean\": true,"
                + "  \"number\": 123,"
                + "  \"string\": \"foo\","
                + "  \"array\": [1, 2]"
                + "}";

        Object result = runtime.parseString(json);

        assertTrue(result instanceof Map, "object");

        Map<String, ?> objectMap = (Map<String, ?>) result;
        assertEquals(true, objectMap.get("boolean"), "boolean property");
        assertEquals(new BigDecimal("123"), objectMap.get("number"), "number property");
        assertEquals("foo", objectMap.get("string"), "string property");

        List<BigDecimal> expectedList = new ArrayList<>();
        expectedList.add(new BigDecimal("1"));
        expectedList.add(new BigDecimal("2"));
        assertEquals(expectedList, objectMap.get("array"), "array property");
    }

    @Test
    void typeOf() {
        assertEquals(JmesPathType.NULL, runtime.typeOf(null), "null");
        assertEquals(JmesPathType.BOOLEAN, runtime.typeOf(true), "boolean");
        assertEquals(JmesPathType.NUMBER, runtime.typeOf(123L), "long");
        assertEquals(JmesPathType.NUMBER, runtime.typeOf(123.45), "double");
        assertEquals(JmesPathType.NUMBER, runtime.typeOf(new BigDecimal("122")), "BigDecimal");
        assertEquals(JmesPathType.STRING, runtime.typeOf(""), "string");
        assertEquals(JmesPathType.ARRAY, runtime.typeOf(Collections.emptyList()), "list");
        assertEquals(JmesPathType.OBJECT, runtime.typeOf(Collections.emptyMap()), "map");
        assertEquals(JmesPathType.OBJECT, runtime.typeOf(exampleStruct()), "struct");
    }

    @Test
    void truthyValues() {
        assertTrue(runtime.isTruthy(true), "true");
        assertTrue(runtime.isTruthy(123L), "long");
        assertTrue(runtime.isTruthy(123.45), "double");
        assertTrue(runtime.isTruthy(new BigDecimal("123.45")), "BigDecimal");
        assertTrue(runtime.isTruthy("foo"), "non-empty string");
        assertTrue(runtime.isTruthy(Collections.singletonList(1)), "non-empty list");
        assertTrue(runtime.isTruthy(Collections.singletonMap(1, 2)), "non-empty map");
        assertTrue(runtime.isTruthy(exampleStruct()), "struct");
    }

    @Test
    void falsyValues() {
        assertFalse(runtime.isTruthy(null), "null");
        assertFalse(runtime.isTruthy(false), "false");
        assertFalse(runtime.isTruthy(""), "empty string");
        assertFalse(runtime.isTruthy(Collections.emptyList()), "empty list");
        assertFalse(runtime.isTruthy(Collections.emptyMap()), "empty map");
    }

    @Test
    void toNumber() {
        assertEquals(123L, runtime.toNumber(123L), "long");
        assertEquals(123.45, runtime.toNumber(123.45), "double");
        assertEquals(new BigDecimal("123.45"), runtime.toNumber(new BigDecimal("123.45")), "BigDecimal");
        assertNull(runtime.toNumber(""), "not a number");
    }

    @Test
    @SuppressWarnings("unchecked")
    void toList() {
        Object input = Collections.singletonList(1);
        List<Object> expected = (List<Object>) input;
        assertEquals(expected, runtime.toList(input), "list");

        input = Collections.singletonMap("a", "b");
        expected = Collections.singletonList("b");
        assertEquals(expected, runtime.toList(input), "map");

        input = exampleStruct();
        expected = new ArrayList<>();
        expected.add("Alice Example");
        expected.add(35);
        assertEquals(expected, runtime.toList(input), "struct");
    }

    @Test
    void toStringMethod() {
        assertEquals("null", runtime.toString(null), "null");
        assertEquals("true", runtime.toString(true), "boolean");
        assertEquals("123", runtime.toString(123L), "long");
        assertEquals("123.45", runtime.toString(123.45), "double");
        assertEquals("123456789123456789123456789123456789.45", runtime.toString(new BigDecimal("123456789123456789123456789123456789.45")), "BigDecimal");
        assertEquals("foo", runtime.toString("foo"), "string");

        List<Object> array = new ArrayList<>();
        array.add(1);
        array.add("\"2\"");
        assertEquals("[1,\"\\\"2\\\"\"]", runtime.toString(array), "array");

        Map<String, Object> object = new HashMap<>();
        object.put("foo", "bar");
        assertEquals("{\"foo\":\"bar\"}", runtime.toString(object), "object");
    }

    @Test
    void createNull() {
        assertNull(runtime.createNull());
    }

    @Test
    void createBoolean() {
        assertEquals(Boolean.TRUE, runtime.createBoolean(true), "true");
        assertEquals(Boolean.FALSE, runtime.createBoolean(false), "false");
    }

    @Test
    void createNumber() {
        assertEquals(123L, runtime.createNumber(123L), "long");
        assertEquals(123.45, runtime.createNumber(123.45), "double");
        assertEquals(new BigDecimal("123.45"), runtime.createNumber(new BigDecimal("123.45")), "BigDecimal");
    }

    @Test
    void createString() {
        assertEquals("foo", runtime.createString("foo"));
    }

    @Test
    void createArray() {
        List<Object> list = Arrays.asList(1, 2, 3);
        assertEquals(list, runtime.createArray(list));
    }

    @Test
    void createObject() {
        Map<Object, Object> map = Collections.singletonMap("foo", "bar");
        assertEquals(map, runtime.createObject(map));
    }

    @Test
    void getProperty() {
        Map<Object, Object> map = Collections.singletonMap("foo", "bar");
        assertEquals("bar", runtime.getProperty(map, (Object) "foo"), "map");

        Struct struct = exampleStruct();
        assertEquals(35, runtime.getProperty(struct, (Object) "age"), "struct");
    }

    @Test
    void getPropertyNames() {
        Map<Object, Object> map = Collections.singletonMap("foo", "bar");
        Collection<Object> expected = Collections.singleton("foo");
        assertEquals(expected, runtime.getPropertyNames(map), "map");

        Struct struct = exampleStruct();
        expected = new ArrayList<>();
        expected.add("name");
        expected.add("age");
        assertEquals(expected, runtime.getPropertyNames(struct), "struct");
    }

    private Struct exampleStruct() {
        Schema schema = SchemaBuilder.struct()
                .name("Person")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();

        return new Struct(schema)
                .put("name", "Alice Example")
                .put("age", 35);
    }
}