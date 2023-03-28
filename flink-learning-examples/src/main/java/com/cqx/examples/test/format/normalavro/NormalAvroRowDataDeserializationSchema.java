package com.cqx.examples.test.format.normalavro;

import com.cqx.examples.test.format.util.GenericRecordUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * AvroRowDataDeserializationSchema
 *
 * @author chenqixu
 */
@PublicEvolving
public class NormalAvroRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private static final Logger logger = LoggerFactory.getLogger(NormalAvroRowDataDeserializationSchema.class);
    private static final long serialVersionUID = 1L;
    protected final RowType rowType;
    protected final TypeInformation<RowData> resultTypeInfo;
    protected final GenericRecordUtil genericRecordUtil;
    protected final String topic;

    public NormalAvroRowDataDeserializationSchema(
            RowType rowType, TypeInformation<RowData> resultTypeInfo,
            String topic, String urlStr, String cluster_name, String group_id) {
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.rowType = checkNotNull(rowType);

        StringBuilder sb = new StringBuilder();
        sb.append("GenericRowData rowData = new GenericRowData(" + rowType.getFieldNames().size() + ");");
        logger.info("{}", sb.toString());
        for (String fieldName : rowType.getFieldNames()) {
            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            logger.info("fieldName={}, subType={}", fieldName, subType);
        }

        logger.info("topic={}, urlStr={}, cluster_name={}, group_id={}", topic, urlStr, cluster_name, group_id);

        this.topic = topic;
        this.genericRecordUtil = new GenericRecordUtil(urlStr, cluster_name, group_id);
        this.genericRecordUtil.addTopic(topic);
    }

    /**
     * 初始化
     *
     * @param context 上下文
     * @throws Exception
     */
    @Override
    public void open(InitializationContext context) throws Exception {
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        GenericRecord genericRecord = genericRecordUtil.binaryToRecord(topic, message);
        GenericRowData genericRowData = new GenericRowData(rowType.getFields().size());
        for (int i = 0; i < rowType.getFields().size(); i++) {
            RowType.RowField rowField = rowType.getFields().get(i);
            String fieldName = rowField.getName();
            LogicalType type = rowField.getType();
            Object genericRecordValue = genericRecord.get(fieldName);
            if (type instanceof BigIntType) {
                genericRowData.setField(i, genericRecordValue);
            } else if (type instanceof FloatType) {
                genericRowData.setField(i, genericRecordValue);
            } else if (type instanceof DoubleType) {
                genericRowData.setField(i, genericRecordValue);
            } else if (type instanceof IntType) {
                genericRowData.setField(i, genericRecordValue);
            } else if (type instanceof VarCharType) {
                StringData stringData = new BinaryStringData(genericRecordValue.toString());
                genericRowData.setField(i, stringData);
            } else if (type instanceof DecimalType) {
                // todo DecimalData
                DecimalData decimalData;
                genericRowData.setField(i, null);
            } else if (type instanceof TimestampType) {
                // todo TimestampData
                TimestampData timestampData;
                genericRowData.setField(i, null);
            }
            logger.debug("fieldName={}, value={}, type={}, typeClass={}"
                    , fieldName, genericRecordValue, type, type.getClass());
        }
        return genericRowData;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NormalAvroRowDataDeserializationSchema that = (NormalAvroRowDataDeserializationSchema) o;
        return Objects.equals(rowType, that.rowType) &&
                Objects.equals(resultTypeInfo, that.resultTypeInfo) &&
                Objects.equals(genericRecordUtil, that.genericRecordUtil) &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, resultTypeInfo, genericRecordUtil, topic);
    }
}
