package com.cqx.examples.agent;

import com.cqx.flink.common.StreamExecutionInf;
import com.cqx.flink.component.HelloWorld;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * RemoteAgent
 *
 * @author chenqixu
 */
public class RemoteAgent {

    public static void main(String[] args) throws Exception {
        String host = "10.1.12.51";
        int port = 8081;
        String[] jarFiles = {
                "D:\\Document\\Workspaces\\Git\\TestFlink\\target\\flink-learning-common-1.0.0.jar"
                , "D:\\Document\\Workspaces\\Git\\TestFlink\\target\\flink-learning-component-1.0.0.jar"
        };
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, jarFiles);
        StreamExecutionInf streamExecutionInf = new HelloWorld();
        streamExecutionInf.exec(env);
    }
}
