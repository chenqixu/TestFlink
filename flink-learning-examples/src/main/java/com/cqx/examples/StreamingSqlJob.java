package com.cqx.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * StreamingSqlJob
 *
 * @author chenqixu
 */
public class StreamingSqlJob {

    public static final String CSV_TABLE_SOURCE_DDL = "" +
            "CREATE TABLE csv_source (\n" +
            " user_id bigint,\n" +
            " item_id bigint,\n" +
            " category_id bigint,\n" +
            " behavior varchar,\n" +
            " ts bigint,\n" +
            " proctime as PROCTIME() \n" +
            ") WITH (\n" +
            " 'connector.type' = 'filesystem', \n" +//指定连接类型
            " 'connector.path' = 'C:\\Users\\tzmaj\\Desktop\\教程\\3\\UserBehavior.csv', \n" +//目录
            " 'format.type' = 'csv', \n" +//文件格式
            " 'format.field-delimiter' = ',' ,-- 字段分隔符 \n" +
            " 'format.fields.0.name' = 'user_id', \n" +//第N字段名，相当于表的schema，索引从0开始
            " 'format.fields.0.data-type' = 'bigint', \n" +//字段类型
            " 'format.fields.1.name' = 'item_id', \n" +
            " 'format.fields.1.data-type' = 'bigint',\n" +
            " 'format.fields.2.name' = 'category_id',\n" +
            " 'format.fields.2.data-type' = 'bigint',\n" +
            " 'format.fields.3.name' = 'behavior', \n" +
            " 'format.fields.3.data-type' = 'String',\n" +
            " 'format.fields.4.name' = 'ts', \n" +
            " 'format.fields.4.data-type' = 'bigint'\n" +
            ")";

    public static final String KAFKA_TABLE_SOURCE_DDL = "" +
            "CREATE TABLE user_behavior (\n" +
            "    user_id BIGINT,\n" +
            "    item_id BIGINT,\n" +
            "    category_id BIGINT,\n" +
            "    behavior STRING,\n" +
            "    ts TIMESTAMP(3)\n" +
            ") WITH (\n" +
            "    'connector.type' = 'kafka', \n" +//指定连接类型是kafka
            "    'connector.version' = '0.11', \n" +//与我们之前Docker安装的kafka版本要一致
            "    'connector.topic' = 'nmc_tb_lte_s1mme_new', \n" +//之前创建的topic
            "    'connector.properties.group.id' = 'flinkDemoGroup', \n" +//消费者组，相关概念可自行百度
            "    'connector.startup-mode' = 'earliest-offset', \n" +//指定从最早消费
            "    'connector.properties.zookeeper.connect' = '10.1.8.200:2181', \n" +//zk地址
            "    'connector.properties.bootstrap.servers' = '10.1.8.200:9092', \n" +//broker地址
            "    'format.type' = 'avro' \n" +//json格式，和topic中的消息格式保持一致
            ")";

    public static void main(String[] args) throws Exception {
        //构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //构建StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);
        //通过DDL，注册kafka数据源表
        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL);
        //执行查询
        Table table = tEnv.sqlQuery("select * from user_behavior");
        //转回DataStream并输出
        tEnv.toAppendStream(table, Row.class).print().setParallelism(1);
        //任务启动，这行必不可少！
        env.execute("StreamingSqlJob");
    }
}
