package com.cqx.learning.formats.avro.util;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.Serializable;

/**
 * SerializableSpecificDatumWriter
 *
 * @author chenqixu
 */
public class SerializableSpecificDatumWriter<T> extends SpecificDatumWriter<T> implements Serializable {

    public SerializableSpecificDatumWriter(Schema schema) {
        super(schema);
    }
}
