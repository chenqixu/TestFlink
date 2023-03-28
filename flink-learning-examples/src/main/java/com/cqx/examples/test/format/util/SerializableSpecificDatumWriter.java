package com.cqx.examples.test.format.util;

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
