package com.cqx.function;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 两个字符串比较，字符串有分隔符，只要s1的某个元素在s2中就为真
 *
 * @author chenqixu
 */
public class ArrayStrCompareFunction extends ScalarFunction {

    public boolean eval(String split, String s1, String s2) {
        if (s1 != null && s2 != null && s1.length() > 0 && s2.length() > 0) {
            String[] a1 = s1.split(split, -1);
            String[] a2 = s2.split(split, -1);
            // 小的集合转List，大的集合做循环
            if (a1.length >= a2.length) {
                List<String> l2 = Arrays.asList(a2);
                for (String t1 : a1) {
                    boolean ret = l2.contains(t1);
                    if (ret) {
                        return true;
                    }
                }
            } else {
                List<String> l1 = Arrays.asList(a1);
                for (String t2 : a2) {
                    boolean ret = l1.contains(t2);
                    if (ret) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
