package com.cqx.examples.test;

import com.cqx.examples.utils.SchemaBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

/**
 * 查询kafka，写入file
 *
 * @author chenqixu
 */
public class KafkaToFileJob {

    public static void main(String[] args) throws Exception {
        // environment configuration
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // register source table in table environment
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.field("city", SchemaBuilder.STRING)
                .field("xdr_id", SchemaBuilder.STRING)
                .field("imsi", SchemaBuilder.STRING)
                .field("imei", SchemaBuilder.STRING)
                .field("msisdn", SchemaBuilder.STRING)
                .field("procedure_type", SchemaBuilder.INT)
                .field("subprocedure_type", SchemaBuilder.STRING)
                .field("procedure_start_time", SchemaBuilder.STRING)
                .field("procedure_delay_time", SchemaBuilder.INT)
                .field("procedure_end_time", SchemaBuilder.STRING)
                .field("procedure_status", SchemaBuilder.INT)
                .field("old_mme_group_id", SchemaBuilder.STRING)
                .field("old_mme_code", SchemaBuilder.STRING)
                .field("lac", SchemaBuilder.INT)
                .field("tac", SchemaBuilder.INT)
                .field("cell_id", SchemaBuilder.INT)
                .field("other_tac", SchemaBuilder.STRING)
                .field("other_eci", SchemaBuilder.STRING)
                .field("home_code", SchemaBuilder.INT)
                .field("msisdn_home_code", SchemaBuilder.INT)
                .field("old_mme_group_id_1", SchemaBuilder.STRING)
                .field("old_mme_code_1", SchemaBuilder.STRING)
                .field("old_m_tmsi", SchemaBuilder.STRING)
                .field("old_tac", SchemaBuilder.STRING)
                .field("old_eci", SchemaBuilder.STRING)
                .field("cause", SchemaBuilder.STRING)
                .field("keyword", SchemaBuilder.STRING)
                .field("mme_ue_s1ap_id", SchemaBuilder.STRING)
                .field("request_cause", SchemaBuilder.STRING)
                .field("keyword_2", SchemaBuilder.STRING)
                .field("keyword_3", SchemaBuilder.STRING)
                .field("keyword_4", SchemaBuilder.STRING)
                .field("bearer_qci1", SchemaBuilder.INT)
                .field("bearer_status1", SchemaBuilder.INT)
                .field("bearer_qci2", SchemaBuilder.INT)
                .field("bearer_status2", SchemaBuilder.INT)
                .field("bearer_qci3", SchemaBuilder.INT)
                .field("bearer_status3", SchemaBuilder.INT)
                .field("user_action_time", SchemaBuilder.TIMESTAMP)
                .field("proc_action_time", SchemaBuilder.TIMESTAMP);

        Schema schemaKafka = schemaBuilder.getSchema();
        String schemaKafkaString = schemaBuilder.getAvroSchemaString("flinkdemo_mc_join_area_v1");
        System.out.println("schemaKafkaString：" + schemaKafkaString);

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("flinkdemo_mc_join_area_v1")
                .property("bootstrap.servers", "10.1.8.200:9092")
                .property("group.id", "flinkDemoGroup")
                .property("security.protocol", "SASL_PLAINTEXT")
                .property("sasl.mechanism", "PLAIN")
                .property("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";"))
                .withSchema(schemaKafka)
                .withFormat(new Avro().avroSchema(schemaKafkaString))
                .createTemporaryTable("flinkdemo_mc_join_area_v1");

        // create an output Table
        final Schema schemaOutPutTable = new Schema()
                .field("diff_time", DataTypes.BIGINT())
                .field("proc_action_time", DataTypes.TIMESTAMP(3))
                .field("user_action_time", DataTypes.TIMESTAMP(3))
                .field("xdr_id", DataTypes.STRING())
                .field("msisdn", DataTypes.STRING());
        tableEnv.connect(new FileSystem().path("d:/tmp/data/flink/csv/file_join_result"))
                .withFormat(new Csv().fieldDelimiter('|').deriveSchema())
                .withSchema(schemaOutPutTable)
                .createTemporaryTable("file_join_result");

        // create a Table object from a SQL query
        Table sqlResult = tableEnv.sqlQuery("select (EXTRACT(MILLISECOND FROM proc_action_time)+\n" +
                "EXTRACT(SECOND FROM proc_action_time)*1000+\n" +
                "EXTRACT(MINUTE FROM proc_action_time)*1000*60+\n" +
                "EXTRACT(HOUR FROM proc_action_time)*1000*60*60) - (\n" +
                "EXTRACT(MILLISECOND FROM user_action_time)+\n" +
                "EXTRACT(SECOND FROM user_action_time)*1000+\n" +
                "EXTRACT(MINUTE FROM user_action_time)*1000*60+\n" +
                "EXTRACT(HOUR FROM user_action_time)*1000*60*60) as diff_time,\n" +
                "proc_action_time,user_action_time,xdr_id,msisdn from flinkdemo_mc_join_area_v1");

        // emit a Table API result Table to a TableSink, same for SQL result
        TableResult tableResult = sqlResult.executeInsert("file_join_result");

        // execute
        tableEnv.execute("SqlSinkCsvFileStream");
    }
}
