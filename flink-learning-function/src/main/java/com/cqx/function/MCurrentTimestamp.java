package com.cqx.function;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * MCurrentTimestamp
 *
 * @author chenqixu
 */
public class MCurrentTimestamp extends ScalarFunction {

    public String eval() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(new Date());
    }

    public long eval(String a) {
        return System.currentTimeMillis();
    }

    public String eval(java.sql.Timestamp timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(timestamp);
    }

    public String eval(java.sql.Timestamp timestamp, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(timestamp);
    }
}
