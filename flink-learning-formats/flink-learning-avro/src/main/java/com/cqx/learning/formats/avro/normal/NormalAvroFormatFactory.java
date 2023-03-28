package com.cqx.learning.formats.avro.normal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * 普通的avro
 *
 * @author chenqixu
 */
public class NormalAvroFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final ConfigOption<String> TOPIC = ConfigOptions.key("topic")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SCHEMA_URL = ConfigOptions.key("schema.url")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SCHEMA_CLUSTER_NAME = ConfigOptions.key("schema.cluster.name")
            .stringType()
            .defaultValue("kafka");
    public static final ConfigOption<String> SCHEMA_GROUP_ID = ConfigOptions.key("schema.group.id")
            .stringType()
            .defaultValue("default");

    /**
     * 反序列化<br>
     * <pre>
     *     #必选
     *     normal-avro.topic
     *     normal-avro.schema.url
     *
     *     #可选
     *     normal-avro.schema.cluster.name
     *     normal-avro.schema.group.id
     * </pre>
     *
     * @param context       上下文
     * @param formatOptions 参数
     * @return
     */
    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final String v_topic = formatOptions.get(TOPIC);
        final String v_schema_url = formatOptions.get(SCHEMA_URL);
        final String v_schema_cluster_name = formatOptions.get(SCHEMA_CLUSTER_NAME);
        final String v_schema_group_id = formatOptions.get(SCHEMA_GROUP_ID);
        return new NormalAvroDecodingFormat(v_topic, v_schema_url, v_schema_cluster_name, v_schema_group_id);
    }

    /**
     * 序列化
     *
     * @param context       上下文
     * @param formatOptions 参数
     * @return
     */
    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        return null;
    }

    /**
     * Factory 的唯一标识
     *
     * @return
     */
    @Override
    public String factoryIdentifier() {
        return "normal-avro";
    }

    /**
     * 必选项
     *
     * @return
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(TOPIC);
        result.add(SCHEMA_URL);
        return result;
    }

    /**
     * 可选项
     *
     * @return
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(SCHEMA_CLUSTER_NAME);
        result.add(SCHEMA_GROUP_ID);
        return result;
    }
}
