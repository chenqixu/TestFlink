package com.cqx.examples.test.sql;

import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * SqlUtil
 *
 * @author chenqixu
 */
public class SqlUtil {

    /**
     * Creates a temporary file with the contents and returns the absolute path.
     */
    public static String createTempFile(String prefix, String contents) throws IOException {
        File tempFile = File.createTempFile(prefix, ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }
}
