package com.cqx.examples.test.format.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * 记录转换器
 *
 * @author chenqixu
 */
public class RecordConvertor implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RecordConvertor.class);
    private final DatumReader<GenericRecord> reader;
    private final DatumWriter<GenericRecord> writer;

    public RecordConvertor(Schema schema) {
        reader = new SerializableSpecificDatumReader<>(schema);
        writer = new SerializableSpecificDatumWriter<>(schema);
    }

    /**
     * byte数组转GenericRecord
     *
     * @param msgByte
     * @return
     */
    public GenericRecord binaryToRecord(byte[] msgByte) {
        Decoder decoder = DecoderFactory.get().binaryDecoder(msgByte, null);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("binaryToRecord异常", e);
        }
    }

    /**
     * GenericRecord转byte数组
     *
     * @param record
     * @return
     */
    public byte[] recordToBinary(GenericRecord record) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new RuntimeException("recordToBinary异常", e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return out.toByteArray();
    }
}
