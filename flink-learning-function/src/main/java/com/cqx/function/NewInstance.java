package com.cqx.function;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 验证调用eval...是否会重复实例化全局变量
 *
 * @author chenqixu
 */
public class NewInstance extends ScalarFunction {
    private Object obj;

    @Override
    public void open(FunctionContext context) {
        obj = new Object();
    }

    @Override
    public void close() {
        obj = null;
    }

    public String eval() {
        if (obj == null) obj = new Object();
        return obj.toString();
    }

    public String eval(String str) {
        if (obj == null) obj = new Object();
        return String.format("v=%s, obj=%s", str, obj.toString());
    }
}
