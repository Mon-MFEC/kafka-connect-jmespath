package de.denisw.kafka.connect.jmespath;

import io.burt.jmespath.BaseRuntime;
import io.burt.jmespath.JmesPathType;
import io.burt.jmespath.jcf.JsonParser;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link io.burt.jmespath.Adapter JMESPath runtime adapter} for the
 * Kafka Connect data types.
 */
public class ConnectJMESPathRuntime extends BaseRuntime<Object> {

    public int compare(Object value1, Object value2) {
        //System.out.println("aaaaaaaa");
        JmesPathType type1 = typeOf(value1);
        JmesPathType type2 = typeOf(value2);
        if (type1 == type2) {
            switch (type1) {
                case NULL:
                    return 0;
                case BOOLEAN:
                    return isTruthy(value1) == isTruthy(value2) ? 0 : -1;
                case NUMBER:
                    //double d1 = toNumber(value1).doubleValue();
                    //double d2 = toNumber(value2).doubleValue();

                    BigDecimal k1 = (BigDecimal) value1;
                    BigDecimal k2 = (BigDecimal) value2;

                    //System.out.println(k1);
                    //System.out.println(k2);

                    return k1.compareTo(k2);
                case STRING:
                    String s1 = toString(value1);
                    String s2 = toString(value2);
                    return s1.compareTo(s2);
                case ARRAY:
                    return deepEqualsArray(value1, value2) ? 0 : -1;
                case OBJECT:
                    return deepEqualsObject(value1, value2) ? 0 : -1;
                default:
                    throw new IllegalStateException(String.format("Unknown node type encountered: %s", value1.getClass().getName()));
            }
        } else {
            return -1;
        }
    }

    private boolean deepEqualsObject(Object value1, Object value2) {
        Collection<Object> keys1 = getPropertyNames(value1);
        Collection<Object> keys2 = getPropertyNames(value2);
        if (keys1.size() != keys2.size()) {
            return false;
        }
        if (!keys1.containsAll(keys2)) {
            return false;
        }
        for (Object key : keys1) {
            if (compare(getProperty(value1, key), getProperty(value2, key)) != 0) {
                return false;
            }
        }
        return true;
    }

    private boolean deepEqualsArray(Object value1, Object value2) {
        List<Object> values1 = toList(value1);
        List<Object> values2 = toList(value2);
        int size = values1.size();
        if (size != values2.size()) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (compare(values1.get(i), values2.get(i)) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Object parseString(String str) {
        return JsonParserBigNumeric.fromString(str, this);
    }

    @Override
    public JmesPathType typeOf(Object value) {
        if (value == null) {
            return JmesPathType.NULL;
        } else if (value instanceof Boolean) {
            return JmesPathType.BOOLEAN;
        } else if (value instanceof Number) {
            return JmesPathType.NUMBER;
        } else if (value instanceof String) {
            return JmesPathType.STRING;
        } else if (value instanceof List) {
            return JmesPathType.ARRAY;
        } else if (value instanceof Map || value instanceof Struct) {
            return JmesPathType.OBJECT;
        } else {
            throw new IllegalStateException("Unexpected value type:" + value.getClass());
        }
    }

    @Override
    public boolean isTruthy(Object value) {
        if (value == null) {
            return false;
        } else if (value instanceof Boolean) {
            return Boolean.TRUE.equals(value);
        } else if (value instanceof Number) {
            return true;
        } else if (value instanceof String) {
            return !((String) value).isEmpty();
        } else if (value instanceof List) {
            return !((Collection<?>) value).isEmpty();
        } else if (value instanceof Map) {
            return !((Map<?, ?>) value).isEmpty();
        } else if (value instanceof Struct) {
            return true;
        } else {
            throw new IllegalStateException("Unexpected value type:" + value.getClass());
        }
    }

    @Override
    public Number toNumber(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        } else {
            return null;
        }
    }

    @Override
    public String toString(Object value) {
        if (value instanceof String) {
            return (String) value;
        } else {
            return toJson(value);
        }
    }



    @Override
    @SuppressWarnings("unchecked")
    public List<Object> toList(Object value) {
        if (value instanceof List) {
            return (List<Object>) value;
        } else if (value instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>) value;
            return new ArrayList<>(map.values());
        } else if (value instanceof Struct) {
            Struct struct = (Struct) value;
            List<Object> fieldValues = new ArrayList<>();
            for (Field field : struct.schema().fields()) {
                fieldValues.add(struct.get(field));
            }
            return fieldValues;
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Object createNull() {
        return null;
    }

    @Override
    public Object createBoolean(boolean b) {
        return b;
    }

    @Override
    public Object createNumber(double n) {


        return n;
    }

    public Object createNumber(BigDecimal b) {
        return b;
    }

    @Override
    public Object createNumber(long n) {
        return n;
    }

    @Override
    public Object createString(String str) {
        return str;
    }

    @Override
    public Object createArray(Collection<Object> elements) {
        return new ArrayList<>(elements);
    }

    @Override
    public Object createObject(Map<Object, Object> obj) {
        return obj;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getProperty(Object value, Object name) {
        if (value instanceof Map) {
            return ((Map<Object, Object>) value).get(name);
        } else if (value instanceof Struct) {
            return ((Struct) value).get((String) name);
        } else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Object> getPropertyNames(Object value) {
        if (value instanceof Map) {
            return ((Map<Object, Object>) value).keySet();
        } else if (value instanceof Struct) {
            return ((Struct) value).schema()
                    .fields()
                    .stream()
                    .map(Field::name)
                    .collect(Collectors.toList());
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private String toJson(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            String escaped = ((String) value).replace("\"", "\\\"");
            return String.format("\"%s\"", escaped);
        } else if (value instanceof List) {
            return toArrayJson((List<Object>) value);
        } else if (value instanceof Map) {
            return toObjectJson((Map<String, Object>) value);
        } else {
            return value.toString();
        }
    }

    private String toArrayJson(List<Object> list) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');

        Iterator<Object> it = list.iterator();
        while (it.hasNext()) {
            sb.append(toJson(it.next()));
            if (it.hasNext()) {
                sb.append(",");
            }
        }

        sb.append(']');
        return sb.toString();
    }

    private String toObjectJson(Map<String, Object> list) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');

        Iterator<Map.Entry<String, Object>> it = list.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            sb.append(toJson(entry.getKey()));
            sb.append(':');
            sb.append(toJson(entry.getValue()));
            if (it.hasNext()) {
                sb.append(",");
            }
        }

        sb.append('}');
        return sb.toString();
    }
}
