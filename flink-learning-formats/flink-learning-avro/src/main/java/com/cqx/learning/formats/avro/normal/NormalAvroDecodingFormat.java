package com.cqx.learning.formats.avro.normal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/**
 * NormalAvroDecodingFormat
 *
 * @author chenqixu
 */
public class NormalAvroDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    protected String topic;
    protected String urlStr;
    protected String cluster_name;
    protected String group_id;

    public NormalAvroDecodingFormat(String topic, String urlStr, String cluster_name, String group_id) {
        this.topic = topic;
        this.urlStr = urlStr;
        this.cluster_name = cluster_name;
        this.group_id = group_id;
    }

    /**
     * 创建一个运行时的反序列化解析器
     *
     * @param context          上下文
     * @param physicalDataType 数据类型
     * @return
     */
    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType physicalDataType) {
        RowType rowtype = (RowType) physicalDataType.getLogicalType();
        TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(physicalDataType);
        return new NormalAvroRowDataDeserializationSchema(rowtype, rowDataTypeInfo, topic, urlStr, cluster_name, group_id);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // 只支持插入数据的时候使用
        return ChangelogMode.insertOnly();
    }
}
