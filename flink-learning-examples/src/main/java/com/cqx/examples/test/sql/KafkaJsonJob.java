package com.cqx.examples.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * KafkaJsonJob
 *
 * @author chenqixu
 */
public class KafkaJsonJob {

    public static void main(String[] args) throws Exception {
        new KafkaJsonJob().run();
    }

    public void run() throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE source_log(\n" +
                        "    message VARCHAR\n" +
                        ") WITH (\n" +
                        "    'connector' = 'kafka',\n" +
                        "    'topic' = 'test1',\n" +
                        "    'properties.bootstrap.servers' = '10.1.8.200:9092',\n" +
                        "    'properties.group.id' = 'group_id',\n" +
                        "    'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                        "    'properties.sasl.mechanism' = 'PLAIN',\n" +
                        "    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";',\n" +
                        "    'scan.startup.mode' = 'group-offsets',\n" +
                        "    'format' = 'json',\n" +
                        "    'json.ignore-parse-errors' = 'true'\n" +
                        ")";
        tEnv.executeSql(ddl);

//        String ddl2 =
//                "CREATE TABLE sink_log (\n" +
//                        "    message VARCHAR\n" +
//                        ") WITH (\n" +
//                        "    'connector' = 'kafka',\n" +
//                        "    'topic' = 'test2',\n" +
//                        "    'properties.bootstrap.servers' = '10.1.8.200:9092',\n" +
//                        "    'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
//                        "    'properties.sasl.mechanism' = 'PLAIN',\n" +
//                        "    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";',\n" +
//                        "    'format' = 'json',\n" +
//                        "    'json.ignore-parse-errors' = 'true'\n" +
//                        ");";
//        tEnv.executeSql(ddl2);

        // run a SQL query on the table and retrieve the result as a new Table
        String query =
                "SELECT message\n" +
                        "FROM source_log";

        tEnv.executeSql(query).print();
    }
}
