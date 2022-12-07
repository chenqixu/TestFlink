package com.cqx.examples.test.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * 时间格式化任务
 *
 * @author chenqixu
 */
public class DateFormatJob extends AbstractSqlJob {

    public static void main(String[] args) throws Exception {
        new DateFormatJob().run();
    }

    @Override
    protected String getSql() {
        return "SELECT "
                + " btime "
                + " ,case when CAST(DATE_FORMAT(TO_TIMESTAMP(SUBSTRING(CAST(btime AS VARCHAR),0,14),'yyyyMMddHHmmss'),'mmss') AS INT) BETWEEN 0 AND 1459 THEN DATE_FORMAT(TO_TIMESTAMP(SUBSTRING(CAST(btime AS VARCHAR),0,14),'yyyyMMddHHmmss'),'yyyyMMddHH')||'15' "
                + "  when CAST(DATE_FORMAT(TO_TIMESTAMP(SUBSTRING(CAST(btime AS VARCHAR),0,14),'yyyyMMddHHmmss'),'mmss') AS INT) BETWEEN 1500 AND 2959 THEN DATE_FORMAT(TO_TIMESTAMP(SUBSTRING(CAST(btime AS VARCHAR),0,14),'yyyyMMddHHmmss'),'yyyyMMddHH')||'30' "
                + "  when CAST(DATE_FORMAT(TO_TIMESTAMP(SUBSTRING(CAST(btime AS VARCHAR),0,14),'yyyyMMddHHmmss'),'mmss') AS INT) BETWEEN 3000 AND 4459 THEN DATE_FORMAT(TO_TIMESTAMP(SUBSTRING(CAST(btime AS VARCHAR),0,14),'yyyyMMddHHmmss'),'yyyyMMddHH')||'45' "
                + "  ELSE DATE_FORMAT(TIMESTAMPADD(HOUR, 1, TO_TIMESTAMP(SUBSTRING(CAST(btime AS VARCHAR),0,14),'yyyyMMddHHmmss')),'yyyyMMddHH')||'00' "
                + "  END as date_partition "
                + " FROM `merge_topic`";
    }

    @Override
    protected Map<String, Table> getNamedCustomersMap() {
        return TableFactory.getInstance()
                // 设置环境变量
                .setTableEnv(getTableEnv())
                //==================
                // 表名：merge_topic
                // 字段：btime - 时间
                //==================
                .newTable()
                // 增加字段
                .addField("btime")
                // 增加数据
                .addRow(Row.of(20220226190000000L))
                .addRow(Row.of(20220226191459000L))
                .addRow(Row.of(20220226191500000L))
                .addRow(Row.of(20220226192959000L))
                .addRow(Row.of(20220226193000000L))
                .addRow(Row.of(20220226194459000L))
                .addRow(Row.of(20220226194500000L))
                .addRow(Row.of(20220226195959000L))
                // 构建表
                .buildTable("merge_topic")
                // 输出
                .getNamedCustomersMap();
    }
}
