package com.cqx.examples.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.Schema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SchemaBuilder
 *
 * @author chenqixu
 */
public class SchemaBuilder {

    public static final String STRING = "string";
    public static final String INT = "int";
    public static final String TIMESTAMP = "timestamp";

    private Map<String, String> fieldMap = new LinkedHashMap<>();

    public SchemaBuilder field(String name, String type) {
        fieldMap.put(name, type);
        return this;
    }

    public Schema getSchema() {
        Schema schemaKafka = new Schema();
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            switch (entry.getValue()) {
                case STRING:
                    schemaKafka.field(entry.getKey(), DataTypes.STRING());
                    break;
                case INT:
                    schemaKafka.field(entry.getKey(), DataTypes.INT());
                    break;
                case TIMESTAMP:
                    schemaKafka.field(entry.getKey(), DataTypes.TIMESTAMP(3));
                    break;
                default:
                    break;
            }
        }
        return schemaKafka;
    }

    public String getAvroSchemaString(String name) {
        StringBuilder builder = new StringBuilder();
        builder.append("{" +
                "  \"type\": \"record\"," +
                "  \"name\": \"" + name + "\"," +
                "  \"fields\" : [");
        int i = 0;
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            if (i > 0) {
                builder.append(",");
            }
            switch (entry.getValue()) {
                case STRING:
                    builder.append("    {\"name\": \"" + entry.getKey() + "\", \"type\": \"" + entry.getValue() + "\"}");
                    break;
                case INT:
                    builder.append("    {\"name\": \"" + entry.getKey() + "\", \"type\": \"" + entry.getValue() + "\"}");
                    break;
                case TIMESTAMP:
                    builder.append("    {\"name\": \"" + entry.getKey() + "\", \"type\": \"long\"}");
                    break;
                default:
                    break;
            }
            i++;
        }
        builder.append("  ]}");
        return builder.toString();
    }
}
