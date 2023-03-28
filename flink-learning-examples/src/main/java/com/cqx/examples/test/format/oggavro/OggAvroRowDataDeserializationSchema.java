package com.cqx.examples.test.format.oggavro;

import com.cqx.examples.test.format.normalavro.NormalAvroRowDataDeserializationSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * OggAvroRowDataDeserializationSchema
 *
 * @author chenqixu
 */
public class OggAvroRowDataDeserializationSchema extends NormalAvroRowDataDeserializationSchema {
    private static final Logger logger = LoggerFactory.getLogger(OggAvroRowDataDeserializationSchema.class);

    public OggAvroRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, String topic, String urlStr, String cluster_name, String group_id) {
        super(rowType, resultTypeInfo, topic, urlStr, cluster_name, group_id);
    }

    private String getStringVal(Object obj) {
        if (obj != null) return obj.toString();
        else return "";
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        GenericRecord genericRecord = genericRecordUtil.binaryToRecord(topic, message);
        // 获取op_type
        String op_type = getStringVal(genericRecord.get("op_type")).toUpperCase();
        if (op_type.equals("I")) {
            // 获取after
            RowData rowAfterData = buildData(genericRecord, false);
            if (rowAfterData != null) {
                rowAfterData.setRowKind(RowKind.INSERT);
                out.collect(rowAfterData);
            }
        } else if (op_type.equals("U")) {
            // update_before
            RowData rowBeforeData = buildData(genericRecord, true);
            if (rowBeforeData != null) {
                rowBeforeData.setRowKind(RowKind.UPDATE_BEFORE);
                out.collect(rowBeforeData);
            }
            // update_after
            RowData rowAfterData = buildData(genericRecord, false);
            if (rowAfterData != null) {
                rowAfterData.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(rowAfterData);
            }
        } else if (op_type.equals("D")) {
            // 获取before
            RowData rowBeforeData = buildData(genericRecord, true);
            if (rowBeforeData != null) {
                rowBeforeData.setRowKind(RowKind.DELETE);
                out.collect(rowBeforeData);
            }
        }
    }

    private RowData buildData(GenericRecord genericRecord, boolean isBefore) {
        GenericRowData genericRowData = new GenericRowData(rowType.getFields().size());
        GenericRecord fieldsRecord;
        if (isBefore) {
            fieldsRecord = (GenericRecord) genericRecord.get("before");
        } else {
            fieldsRecord = (GenericRecord) genericRecord.get("after");
        }
        for (int i = 0; i < rowType.getFields().size(); i++) {
            RowType.RowField rowField = rowType.getFields().get(i);
            String fieldName = rowField.getName();
            LogicalType type = rowField.getType();
            Object genericRecordValue = fieldsRecord.get(fieldName);
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
            logger.debug("isBefore={}, fieldName={}, value={}, type={}, typeClass={}"
                    , isBefore, fieldName, genericRecordValue, type, type.getClass());
        }
        return genericRowData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OggAvroRowDataDeserializationSchema that = (OggAvroRowDataDeserializationSchema) o;
        return Objects.equals(rowType, that.rowType) &&
                Objects.equals(resultTypeInfo, that.resultTypeInfo) &&
                Objects.equals(genericRecordUtil, that.genericRecordUtil) &&
                Objects.equals(topic, that.topic);
    }
}
