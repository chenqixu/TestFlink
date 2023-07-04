package com.cqx.examples.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * Top-N
 *
 * @author chenqixu
 */
public class TopNJob {

    public static void main(String[] args) throws Exception {
        new TopNJob().run();
    }

    public void run() throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "13500001234,厦门,2019-12-11 23:59:03\n"
                        + "13500001234,连江,2019-12-12 00:00:03\n"
                        + "13500001234,台江,2019-12-12 00:00:02\n"
                        + "13500001234,台江,2019-12-12 00:00:01\n"
                        + "13500001234,马尾,2019-12-12 00:00:04\n"
                        + "13500001234,鼓楼,2019-12-12 00:00:06\n"
                        + "13500001234,晋安,2019-12-12 00:00:05\n"
                        + "13500001234,闽侯,2019-12-12 00:00:08";
        String path = SqlUtil.createTempFile("orders", contents);
        System.out.println(String.format("path=%s", path));

        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE orders (\n"
                        + "  msisdn STRING,\n"
                        + "  location STRING,\n"
                        + "  btime TIMESTAMP(3),\n"
                        + "  WATERMARK FOR btime AS btime - INTERVAL '5' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(ddl);

        // run a SQL query on the table and retrieve the result as a new Table
        // 仅开窗
//        String query =
//                "SELECT window_start, window_end, msisdn, btime "
//                        + " from TABLE( "
//                        + " TUMBLE(TABLE orders, DESCRIPTOR(btime), INTERVAL '5' SECOND) "
//                        + " ) "
//                        + " group by window_start, window_end, msisdn, btime";

        // 开窗+排序
        String query = "SELECT * FROM ( "
                + " SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end, msisdn ORDER BY btime) as rownum "
                + " FROM ( "
                + " SELECT window_start, window_end, msisdn, btime "
                + " from TABLE( "
                + " TUMBLE(TABLE orders, DESCRIPTOR(btime), INTERVAL '5' SECOND) "
                + " ) "
                + " group by window_start, window_end, msisdn, btime"
                + " ) "
                + " ) WHERE rownum = 1 ";

        tEnv.executeSql(query).print();
    }
}
