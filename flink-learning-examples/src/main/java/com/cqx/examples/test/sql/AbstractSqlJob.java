package com.cqx.examples.test.sql;

import com.cqx.examples.test.function.ConvFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;

/**
 * AbstractSqlJob
 *
 * @author chenqixu
 */
public abstract class AbstractSqlJob {
    private TableEnvironment tableEnv;
    private StreamExecutionEnvironment streamEnv;

    /**
     * 模拟的表、字段、数据
     *
     * @return
     */
    protected abstract Map<String, Table> getNamedCustomersMap();

    /**
     * 需要运行的sql
     *
     * @return
     */
    protected abstract String getSql();

    public void run() throws Exception {
        run(JobModel.BATCH);
    }

    public void run(JobModel jobModel) throws Exception {
        if (jobModel.equals(JobModel.BATCH) || jobModel.equals(JobModel.STREAM1)) {
            EnvironmentSettings settings;
            if (jobModel.equals(JobModel.STREAM1)) {
                // in this case: declare that the table programs should be executed in stream mode
                settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            } else {
                // in this case: declare that the table programs should be executed in batch mode
                settings = EnvironmentSettings.newInstance().inBatchMode().build();
            }
            // set up the Java Table API
            tableEnv = TableEnvironment.create(settings);

            // 注册函数
            tableEnv.createTemporarySystemFunction("CONV", ConvFunction.class);

            // register a view temporarily
            for (Map.Entry<String, Table> entry : getNamedCustomersMap().entrySet()) {
                tableEnv.createTemporaryView(entry.getKey(), entry.getValue());
            }

            // use SQL whenever you like
            // call execute() and print() to get insights
            tableEnv.sqlQuery(getSql())
                    .execute()
                    .print();
        } else if (jobModel.equals(JobModel.STREAM2)) {
            // set up the Java DataStream API
            streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            // set up the Java Table API
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
            this.tableEnv = tableEnv;

            // 注册函数
            tableEnv.createTemporarySystemFunction("CONV", ConvFunction.class);

            // register a view temporarily
            for (Map.Entry<String, Table> entry : getNamedCustomersMap().entrySet()) {
                tableEnv.createTemporaryView(entry.getKey(), entry.getValue());
            }

            // sqlQuery
            Table result = tableEnv.sqlQuery(getSql());

            // convert the Table back to an insert-only DataStream of type `Order`
            tableEnv.toDataStream(result).print();

            // after the table program is converted to a DataStream program,
            // we must use `tableEnv.execute()` to submit the job
            streamEnv.execute();
        }
    }

    public TableEnvironment getTableEnv() {
        return tableEnv;
    }

    public StreamExecutionEnvironment getStreamEnv() {
        return streamEnv;
    }
}
