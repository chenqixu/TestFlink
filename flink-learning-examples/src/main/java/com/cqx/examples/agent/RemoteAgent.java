package com.cqx.examples.agent;

import com.cqx.flink.common.StreamExecutionInf;
import com.cqx.flink.component.HelloWorld;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * RemoteAgent
 *
 * @author chenqixu
 */
public class RemoteAgent {
    private static final String HOST = "10.1.12.51";
    private static final int JOBMANAGER_PORT = 6123;
    private static final int REST_PORT = 8081;

    public static void main(String[] args) throws Exception {
        new RemoteAgent().envExec();
    }

    public void envExec() throws Exception {
        String[] jarFiles = {
                "I:\\Document\\Workspaces\\Git\\TestFlink\\target\\flink-learning-common-1.0.0.jar"
                , "I:\\Document\\Workspaces\\Git\\TestFlink\\target\\flink-learning-component-1.0.0.jar"
        };
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(HOST, REST_PORT, jarFiles);
        StreamExecutionInf streamExecutionInf = new HelloWorld();
        streamExecutionInf.exec(env);
    }

    public void clientSubmit() {
        try {
            // 配置standalone集群信息
            Configuration config = new Configuration();
            config.setString(JobManagerOptions.ADDRESS, HOST);
            config.setInteger(JobManagerOptions.PORT, JOBMANAGER_PORT);
            config.setInteger(RestOptions.PORT, REST_PORT);
//            config.setString(PipelineOptions.NAME,"Filter Adults Job");
            RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(config, StandaloneClusterId.getInstance());

            // Job运行的配置
            int parallelism = 1;
            SavepointRestoreSettings savePoint = SavepointRestoreSettings.none();

            // todo 缺jarFile
            // 设置job的入口和参数
            File jarFile = new File("");
            PackagedProgram program = PackagedProgram
                    .newBuilder()
                    .setConfiguration(config)
                    .setJarFile(jarFile)
                    // todo 缺PointClassName
                    .setEntryPointClassName("")
                    .setSavepointRestoreSettings(savePoint)
                    .build();

            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, parallelism, false);
            CompletableFuture<JobID> result = client.submitJob(jobGraph);
            JobID jobId = result.get();
            System.out.println("job: [" + jobId.toHexString() + "] 提交完成！");
            System.out.println("job: [" + jobId.toHexString() + "] 是否执行完成：" + result.isDone());
            System.out.println("job: [" + jobId.toHexString() + "] 是否异常结束：" + result.isCompletedExceptionally());
            System.out.println("job: [" + jobId.toHexString() + "] 是否取消：" + result.isCancelled());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
