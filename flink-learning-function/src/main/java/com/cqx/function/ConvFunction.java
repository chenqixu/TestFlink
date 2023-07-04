package com.cqx.function;


import org.apache.flink.table.functions.ScalarFunction;

/**
 * 进制转换，其他进制转换成十进制Integer
 */
public class ConvFunction extends ScalarFunction {

    public Integer eval(String s, int radix) {
        int result = 0;
        try {
            result = Integer.valueOf(s, radix);
            return result;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return null;
        }
    }
}