package com.cqx.examples.test.format.oggavro;

import com.cqx.examples.test.format.normalavro.NormalAvroDecodingFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * OggAvroDecodingFormat
 *
 * @author chenqixu
 */
public class OggAvroDecodingFormat extends NormalAvroDecodingFormat {

    public OggAvroDecodingFormat(String topic, String urlStr, String cluster_name, String group_id) {
        super(topic, urlStr, cluster_name, group_id);
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
        return new OggAvroRowDataDeserializationSchema(rowtype, rowDataTypeInfo, topic, urlStr, cluster_name, group_id);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
