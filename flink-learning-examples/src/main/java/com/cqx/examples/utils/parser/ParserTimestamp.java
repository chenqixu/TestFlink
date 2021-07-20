package com.cqx.examples.utils.parser;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ParserTimestamp
 * 时间转换成yyyyMMddHHmmss
 *
 * @author chenqixu
 */
public class ParserTimestamp implements IStreamParser {

    private String param;
    // 输出格式
    private final DateFormat dateoutFormat;

    public ParserTimestamp(String param) {
        this.param = param;
        this.dateoutFormat = new SimpleDateFormat(param);
    }

    @Override
    public String parser(String value) throws Exception {
        if (value != null && value.length() > 0) {
            // 时间戳转换成输出
            String result = null;
            try {
                // 高并发情况下必须同步simpleDateFormat
                synchronized (dateoutFormat) {
                    result = dateoutFormat.format(new Date(Long.valueOf(value)));
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new Exception("时间戳转换异常，" + e.getMessage());
            }
            return result;
        } else {
            return null;
        }
    }
}
