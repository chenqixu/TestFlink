package com.cqx.learning.formats.avro.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class GenericRecordUtilTest {
    private static final Logger logger = LoggerFactory.getLogger(GenericRecordUtilTest.class);
    private GenericRecordUtil genericRecordUtil;

    @org.junit.Before
    public void setUp() throws Exception {
        genericRecordUtil = new GenericRecordUtil("http://10.1.8.200:8080/SchemaService/getSchema?t=", "kafka", "default");
        genericRecordUtil.addTopic("test1");
        genericRecordUtil.addTopic("USER_PRODUCT");
    }

    @org.junit.After
    public void tearDown() throws Exception {
    }

    @org.junit.Test
    public void binaryToRecord() {
        byte[] message = {0, 12, -25, -90, -113, -27, -73, -98, 0, 10, 49, 50, 51, 52, 53, 0, 8, 48, 120, 48, 48, 0, 8, 48, 120, 48, 49, 0, 22, 49, 51, 53, 48, 48, 48, 48, 48, 48, 48, 48};
        GenericRecord genericRecord = genericRecordUtil.binaryToRecord("test1", message);
        logger.info("genericRecord={}", genericRecord);
    }

    @Test
    public void flatTest() {
        Schema sc = genericRecordUtil.getSchema("USER_PRODUCT");
//        FlatUtil flatUtil = new FlatUtil("USER_PRODUCT", sc, null);
        // 去掉sc的before和after
        Schema flatSc = new Schema.Parser().parse("{\"type\" : \"record\",  \"name\" : \"tb_ser_ogg_user_product\",  \"namespace\" : \"frtbase\", \"fields\" : []}");
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("id", Schema.create(Schema.Type.INT)));
        fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING)));
        flatSc.setFields(fields);
        logger.info("flatSc={}", flatSc);
    }
}