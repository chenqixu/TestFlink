package com.cqx.examples.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * OggAvroJob
 *
 * @author chenqixu
 */
public class OggAvroJob {

    public static void main(String[] args) {
        new OggAvroJob().run();
    }

    public void run() {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE USER_PRODUCT ("
                        + "  HOME_CITY BIGINT"
                        + "  ,PRODUCT_TYPE BIGINT"
                        + "  ,EXPIRE_TIME STRING"
                        + "  ,REQUEST_SOURCE BIGINT"
                        + "  ,STATUS BIGINT"
                        + "  ,CREATE_TIME STRING"
                        + ") WITH (\n"
                        + "'connector' = 'kafka'\n"
                        + ",'topic' = 'USER_PRODUCT'\n"
                        + ",'properties.bootstrap.servers' = '10.1.8.200:9092'\n"
                        + ",'properties.security.protocol' = 'SASL_PLAINTEXT'\n"
                        + ",'properties.sasl.mechanism' = 'PLAIN'\n"
                        + ",'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";'\n"
                        + ",'properties.group.id' = 'testGroup'\n"
                        + ",'format' = 'ogg-avro'\n"
                        + ",'ogg-avro.schema.url' = 'http://10.1.8.200:8080/SchemaService/getSchema?t='\n"
                        + ",'ogg-avro.schema.cluster.name' = 'kafka'\n"
                        + ",'ogg-avro.schema.group.id' = 'default'\n"
                        + ",'ogg-avro.topic' = 'USER_PRODUCT'\n"
                        + ")";
        tEnv.executeSql(ddl);

        // run a SQL query
        String query =
                "SELECT HOME_CITY,PRODUCT_TYPE,EXPIRE_TIME,REQUEST_SOURCE,STATUS,CREATE_TIME FROM USER_PRODUCT";

        tEnv.executeSql(query).print();
    }
}
