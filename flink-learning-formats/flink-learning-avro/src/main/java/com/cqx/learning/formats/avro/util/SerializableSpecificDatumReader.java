package com.cqx.learning.formats.avro.util;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * SerializableSpecificDatumReader<br>
 * <pre>
 *     由于SpecificDatumReader不支持序列化
 *     所以SpecificDatumReader的所有属性都没有序列化
 *     继承之后也不行，只能重写writeObject和readObject
 *     在writeObject中把schema写入
 *     在readObject把schema读到，并设置到父类
 * </pre>
 *
 * @author chenqixu
 */
public class SerializableSpecificDatumReader<T> extends SpecificDatumReader<T> implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SerializableSpecificDatumReader.class);
    private Schema serializableSchema;

    public SerializableSpecificDatumReader(Schema schema) {
        super(schema);
        this.serializableSchema = schema;
        logger.info("[SerializableSpecificDatumReader.build], get.schema={}", schema);
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.writeObject(getSerializableSchema());
        logger.info("[SerializableSpecificDatumReader.writeObject], serializableSchema={}", getSerializableSchema());
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        Schema _serializableSchema = (Schema) in.readObject();
        setSerializableSchema(_serializableSchema);
        setSchema(_serializableSchema);
        logger.info("[SerializableSpecificDatumReader.readObject], serializableSchema={}", getSerializableSchema());
    }

    public Schema getSerializableSchema() {
        return serializableSchema;
    }

    public void setSerializableSchema(Schema serializableSchema) {
        this.serializableSchema = serializableSchema;
    }
}
