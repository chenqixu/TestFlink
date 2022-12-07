package com.cqx.examples.test.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * 行转列
 *
 * @author chenqixu
 */
public class ColumnToRowJob extends AbstractSqlJob {

    public static void main(String[] args) throws Exception {
        new ColumnToRowJob().run();
    }

    @Override
    protected String getSql() {
        return "SELECT "
                + " name "
                + " ,course "
                + " ,score "
                + " FROM `test_topic` "
                + " CROSS JOIN UNNEST(`list`) AS t (course,score)";
    }

    @Override
    protected Map<String, Table> getNamedCustomersMap() {
        return TableFactory.getInstance()
                // 设置环境变量
                .setTableEnv(getTableEnv())
                //==================
                // 表名：test_topic
                // 字段：name - 名字
                // 字段：list - json
                //==================
                .newTable()
                // 增加字段
                .addField("name")
                .addField("list")
                // 增加数据
                .addRow(Row.of("小张", new Row[]{Row.of("flink", "99"), Row.of("spark", "88"), Row.of("hadoop", "77")}))

//        //============================
//        // 等于如下效果
//        //============================
//        tableEnv.fromValues(
//                DataTypes.ROW(
//                        DataTypes.FIELD("name", DataTypes.STRING()),
//                        DataTypes.FIELD("list"
//                                , DataTypes.ARRAY(
//                                        DataTypes.ROW(
//                                                DataTypes.FIELD("course", DataTypes.STRING())
//                                                , DataTypes.FIELD("score", DataTypes.INT())
//                                        )
//                                )
//                        )
//                ),
//                Row.of("小张", new Row[]{Row.of("flink", "99"), Row.of("spark", "88"), Row.of("hadoop", "77")})
//        );

                // 构建表
                .buildTable("test_topic")
                // 输出
                .getNamedCustomersMap();
    }
}
