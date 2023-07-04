package com.cqx.examples.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * 字符串比较
 *
 * @author chenqixu
 */
public class StrCompareJob {

    public static void main(String[] args) throws Exception {
        new StrCompareJob().run();
    }

    public void run() throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String func = "CREATE FUNCTION ArrayStrCompare AS 'com.cqx.function.ArrayStrCompareFunction'";
        tEnv.executeSql(func);

        String view1 = "create view t1(id,s) as values (111,'1,2,3')";
        tEnv.executeSql(view1);

        String view2 = "create view t2(id,s) as values (111,'3,4,5,6')";
        tEnv.executeSql(view2);

        String query = "select t1.id,t1.s as t1s,t2.s as t2s,ArrayStrCompare(',',t1.s,t2.s) as ret from t1 left join t2 on t1.id=t2.id";
        tEnv.executeSql(query).print();
    }
}
