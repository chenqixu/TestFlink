package com.cqx.examples.utils.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * StreamParserUtil
 *
 * @author chenqixu
 */
public class StreamParserUtil {
    private static final Logger logger = LoggerFactory.getLogger(StreamParserUtil.class);
    private static Map<String, IStreamParser> parserMap = new HashMap<>();
    private static ParserConvertHex parserConvertHex = new ParserConvertHex("10");
    private static ParserSubstrFirst parserSubstrFirst = new ParserSubstrFirst("86");
    private static ParserTimestamp parserTimestamp = new ParserTimestamp("yyyyMMddHHmmss");

    static {
        parserMap.put("parserConvertHex-10", parserConvertHex);
        parserMap.put("parserSubstrFirst-86", parserSubstrFirst);
        parserMap.put("parserTimestamp-yyyyMMddHHmmss", parserTimestamp);
    }

    public static String parser(String rule, String value) throws Exception {
        String result = value;
        IStreamParser iStreamParser = parserMap.get(rule);
        if (iStreamParser != null) {
            String begin = value;
            result = iStreamParser.parser(result);
            logger.debug("parserRule：{}，start：{}，end：{}", rule, begin, result);
        }
        return result;
    }

}
