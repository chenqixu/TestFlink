package com.cqx.flink.agent;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * YamlParser
 *
 * @author chenqixu
 */
public class YamlParser {

    public Map parserConf(String path) throws IOException {
        Yaml yaml;
        InputStream is = null;
        Map<?, ?> map;
        try {
            // 加载yaml配置文件
            yaml = new Yaml();
            is = new FileInputStream(path);
            map = yaml.loadAs(is, Map.class);
            is.close();
        } finally {
            if (is != null)
                is.close();
        }
        return map;
    }
}
