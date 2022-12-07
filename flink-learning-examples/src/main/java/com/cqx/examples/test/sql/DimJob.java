package com.cqx.examples.test.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * 维表关联任务
 *
 * @author chenqixu
 */
public class DimJob extends AbstractSqlJob {

    public static void main(String[] args) throws Exception {
        new DimJob().run();
    }

    @Override
    protected String getSql() {
        return "select "
                + " t.imsi "
                + " ,t.msisdn "
                + " ,substring(CAST(t.imsi AS VARCHAR),1,3) as mcc "
                + " ,substring(CAST(t.msisdn AS VARCHAR),1,7) as msisdn7 "
                + " ,substring(CAST(t.msisdn AS VARCHAR),1,8) as msisdn8 "
                + " ,case when t1.msisdn is not null then t1.home_code "
                + "when t2.city is not null then t2.city "
                + "when t3.city is not null then t3.city "
                + "when t4.nation is not null then t4.nation else '未知' end as location "
                + " from test1 t "
                // 用户归属
                + " left join dim_user_info t1 on t.msisdn=t1.msisdn "
                // 号段7-地市、省份
                + " left join dim_city t2 on substring(CAST(t.imsi AS VARCHAR),1,3) in('460','461') and t2.msisdn78=substring(CAST(t.msisdn AS VARCHAR),1,7) "
                // 号段8-地市、省份
                + " left join dim_city t3 on substring(CAST(t.imsi AS VARCHAR),1,3) in('460','461') and t3.msisdn78=substring(CAST(t.msisdn AS VARCHAR),1,8) "
                // 移动国家码-国家
                + " left join dim_nation t4 on substring(CAST(t.imsi AS VARCHAR),1,3)=t4.mcc "
                ;
    }

    @Override
    protected Map<String, Table> getNamedCustomersMap() {
        return TableFactory.getInstance()
                // 设置环境变量
                .setTableEnv(getTableEnv())
                //==================
                // 表名：test1
                // 字段：imsi - 国际移动用户识别码
                // 字段：misisdn - 手机号码
                //==================
                .newTable()
                // 增加字段
                .addField("imsi")
                .addField("msisdn")
                // 增加数据
                .addRow(Row.of(460111, 13000000001L))
                .addRow(Row.of(461111, 13000010002L))
                .addRow(Row.of(466111, 13000000003L))
                .addRow(Row.of(455111, 13000000004L))
                .addRow(Row.of(454111, 13000000005L))
                .addRow(Row.of(461111, 13000001006L))
                .addRow(Row.of(462111, 13000000007L))
                .addRow(Row.of(463111, 13000000008L))
                .addRow(Row.of(464111, 130000000090L))
                .addRow(Row.of(461111, 13000110010L))
                // 构建表
                .buildTable("test1")
                //==================
                // 表名：dim_user_info
                // 字段：misisdn - 手机号码
                //==================
                .newTable()
                .addField("msisdn")
                .addField("home_code")
                .addRow(Row.of(13000000001L, "福州"))
                .buildTable("dim_user_info")
                //==================
                // 表名：dim_city
                // 字段：msisdn78 - 手机号码前7、8位
                // 字段：city - 地市
                //==================
                .newTable()
                .addField("msisdn78")
                .addField("city")
                .addRow(Row.of("1300001", "地市1"))
                .addRow(Row.of("13000001", "地市2"))
                .buildTable("dim_city")
                //==================
                // 表名：dim_nation
                // 字段：mcc - 移动国家码
                // 字段：nation - 国家
                //==================
                .newTable()
                .addField("mcc")
                .addField("nation")
                .addRow(Row.of("463", "国家1"))
                .addRow(Row.of("454", "香港"))
                .addRow(Row.of("455", "澳门"))
                .addRow(Row.of("466", "台湾"))
                .buildTable("dim_nation")
                // 输出
                .getNamedCustomersMap();
    }
}
