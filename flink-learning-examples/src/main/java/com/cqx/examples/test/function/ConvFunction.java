package com.cqx.examples.test.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 其他进制转换成十进制Integer
 *
 * @author chenqixu
 */
public class ConvFunction extends ScalarFunction {

    public Integer eval(String s, int radix) {
        return Integer.valueOf(s, radix);
    }
}
