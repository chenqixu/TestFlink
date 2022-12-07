package com.cqx.examples.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * StreamJoinJob
 *
 * @author chenqixu
 */
public class StreamJoinJob {

    public static void main(String[] args) throws Exception {
        new StreamJoinJob().run();
    }

    public void run() throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "1,厦门,2019-12-11 23:59:03\n"
                        + "1,连江,2019-12-12 00:00:03\n"
                        + "1,台江,2019-12-12 00:00:02\n"
                        + "1,台江,2019-12-12 00:00:01\n"
                        + "1,马尾,2019-12-12 00:00:04\n"
                        + "1,鼓楼,2019-12-12 00:00:06\n"
                        + "1,晋安,2019-12-12 00:00:05\n"
                        + "1,闽侯,2019-12-12 00:00:08";
        String path = SqlUtil.createTempFile("orders", contents);
        String path2 = SqlUtil.createTempFile("orders2", contents);

        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE orders (\n"
                        + "  user_id INT,\n"
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

        String ddl2 =
                "CREATE TABLE orders2 (\n"
                        + "  user_id INT,\n"
                        + "  location STRING,\n"
                        + "  btime TIMESTAMP(3),\n"
                        + "  WATERMARK FOR btime AS btime - INTERVAL '5' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path2
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(ddl2);

//        String view = "CREATE VIEW v_order as \n"
//                + " SELECT \n"
//                + " max(btime) max_btime, \n"
//                + " user_id \n"
//                + "FROM orders \n"
//                + "GROUP BY user_id,TUMBLE(btime, INTERVAL '5' SECOND)";
//        tEnv.executeSql(view);

        // run a SQL query on the table and retrieve the result as a new Table
        String query =
                "SELECT btime,user_id,location FROM (" +
                        "SELECT \n"
//                        + " window_start, \n"
//                        + " window_end, \n"
//                        + " window_time, \n"
                        + " btime, \n"
                        + " user_id, \n"
                        + " location, \n"
//                        + " ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY btime) as o1 "
                        + " FIRST_VALUE(location) OVER (PARTITION BY user_id ORDER BY btime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as o1, "
//                        + " LAST_VALUE(location) OVER (PARTITION BY user_id ORDER BY btime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as o2, "
                        + " COUNT(user_id) OVER (PARTITION BY user_id ORDER BY btime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as cn "
                        + "FROM orders ) WHERE o1<>location";

//        String query =
//                "SELECT \n"
//                        + " btime, \n"
//                        + " t1.user_id, \n"
//                        + " location \n"
//                        + " FROM orders2 t1 "
//                        + " LEFT JOIN v_order FOR SYSTEM_TIME AS OF t1.btime AS t2 "
//                        + " ON t1.btime=t2.max_btime ";

//        "SELECT window_start, window_end,count(user_id),LAST_VALUE(location) as last_location over(part),FIRST_VALUE(location) as first_location FROM TABLE (" +
//                " TUMBLE(TABLE orders, DESCRIPTOR(btime), INTERVAL '5' SECOND))" +
//                " GROUP BY window_start, window_end";

//                "select * from orders order by btime";

        tEnv.executeSql(query).print();
    }
}
