package com.cqx.flink.agent;

import com.cqx.flink.common.StreamExecutionInf;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * RemoteAgent
 *
 * @author chenqixu
 */
public class RemoteAgent {

    /**
     * 读取yaml配置文件，提交任务
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            String path = args[0];
            File conf = new File(path);
            if (conf.isFile() && conf.exists()) {
                //解析配置文件
                YamlParser yamlParser = new YamlParser();
                Map params = yamlParser.parserConf(path);
                //flink
                Map<String, ?> flink = (Map<String, ?>) params.get("flink");
                String host = (String) flink.get("host");
                int port = (Integer) flink.get("port");
                List<Map> jarfileList = (List) flink.get("jarflies");
                String[] jarFiles = new String[jarfileList.size()];
                for (int i = 0; i < jarfileList.size(); i++) {
                    jarFiles[i] = (String) jarfileList.get(i).get("file");
                }
                //job
                Map<String, ?> job = (Map<String, ?>) params.get("job");
                String name = (String) job.get("name");
                String packagename = (String) job.get("packagename");
                //param
                Map<String, ?> param = (Map<String, ?>) params.get("param");

                System.out.println(String.format("host:%s, port:%s, jarfiles:%s, job.name:%s, job.packagename:%s, param:%s",
                        host, port, jarfileList, name, packagename, param));
                //创建远程环境
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, jarFiles);
                //构造执行类
                Class cls = Class.forName(packagename + "." + name);
                StreamExecutionInf streamExecutionInf = (StreamExecutionInf) cls.newInstance();
                //初始化参数
                streamExecutionInf.init(param);
                //提交任务执行
                streamExecutionInf.exec(env);
            } else {
                System.out.println("conf file is not exists , please check.");
            }
        } else {
            System.out.println("you need input conf , please check.");
        }
    }
}
