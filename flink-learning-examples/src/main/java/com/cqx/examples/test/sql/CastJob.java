package com.cqx.examples.test.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * 进制转换任务
 *
 * @author chenqixu
 */
public class CastJob extends AbstractSqlJob {

    public static void main(String[] args) throws Exception {
        System.out.println(Integer.toHexString(15));
        System.out.println(Integer.valueOf(Integer.toHexString(15), 16));
        new CastJob().run();
    }

    @Override
    protected String getSql() {
        return "select h16,CONV(h16, 16) as conv " +
                " ,case when 1=2 then case when 1=1 then 2 else 0 end else 1 end as ca " +
                " from test1 ";
    }

    @Override
    protected Map<String, Table> getNamedCustomersMap() {
        return TableFactory.getInstance()
                // 设置环境变量
                .setTableEnv(getTableEnv())
                //==================
                // 表名：test1
                // 字段：h16 - 16进制
                //==================
                .newTable()
                // 增加字段
                .addField("h16")
                // 增加数据
                .addRow(Row.of("1a"))
                // 构建表
                .buildTable("test1")
                // 输出
                .getNamedCustomersMap();
    }
}
