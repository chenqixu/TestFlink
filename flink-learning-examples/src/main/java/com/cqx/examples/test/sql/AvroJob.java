package com.cqx.examples.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * AvroJob
 *
 * @author chenqixu
 */
public class AvroJob {

    public static void main(String[] args) {
        new AvroJob().run();
    }

    public void run() {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE test1 (\n" +
                        "  city STRING,\n" +
                        "  xdr_id STRING,\n" +
                        "  imsi STRING,\n" +
                        "  imei STRING,\n" +
                        "  msisdn STRING"
                        + ") WITH (\n"
                        + "'connector' = 'kafka'\n"
                        + ",'topic' = 'test1'\n"
                        + ",'properties.bootstrap.servers' = '10.1.8.200:9092'\n"
                        + ",'properties.security.protocol' = 'SASL_PLAINTEXT'\n"
                        + ",'properties.sasl.mechanism' = 'PLAIN'\n"
                        + ",'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";'\n"
                        + ",'properties.group.id' = 'testGroup'\n"
                        + ",'format' = 'normal-avro'\n"
                        + ",'normal-avro.schema.url' = 'http://10.1.8.200:8080/SchemaService/getSchema?t='\n"
                        + ",'normal-avro.schema.cluster.name' = 'kafka'\n"
                        + ",'normal-avro.schema.group.id' = 'default'\n"
                        + ",'normal-avro.topic' = 'test1'\n"
                        + ")";
        tEnv.executeSql(ddl);

        // run a SQL query
        String query =
                "SELECT city,xdr_id,imsi,imei,msisdn FROM test1";

        tEnv.executeSql(query).print();
    }
}
