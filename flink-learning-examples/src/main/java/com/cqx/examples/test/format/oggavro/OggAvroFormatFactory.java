package com.cqx.examples.test.format.oggavro;

import com.cqx.examples.test.format.normalavro.NormalAvroFormatFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * ogg下的avro
 *
 * @author chenqixu
 */
public class OggAvroFormatFactory extends NormalAvroFormatFactory {

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final String v_topic = formatOptions.get(TOPIC);
        final String v_schema_url = formatOptions.get(SCHEMA_URL);
        final String v_schema_cluster_name = formatOptions.get(SCHEMA_CLUSTER_NAME);
        final String v_schema_group_id = formatOptions.get(SCHEMA_GROUP_ID);
        return new OggAvroDecodingFormat(v_topic, v_schema_url, v_schema_cluster_name, v_schema_group_id);
    }

    @Override
    public String factoryIdentifier() {
        return "ogg-avro";
    }
}
