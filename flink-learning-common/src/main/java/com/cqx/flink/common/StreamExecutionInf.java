package com.cqx.flink.common;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * StreamExecutionInf
 *
 * @author chenqixu
 */
public interface StreamExecutionInf {

    void init(Map params) throws Exception;

    JobExecutionResult exec(StreamExecutionEnvironment env) throws Exception;
}
