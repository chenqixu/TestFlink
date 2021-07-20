package com.cqx.flink.component;

import com.cqx.flink.common.StreamExecutionInf;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * HelloWorld
 *
 * @author chenqixu
 */
public class HelloWorld implements StreamExecutionInf {

    @Override
    public void init(Map params) throws Exception {

    }

    @Override
    public JobExecutionResult exec(StreamExecutionEnvironment env) throws Exception {
        //创建DataSource
        String[] words = {"apple", "orange", "banana", "watermelon"};
        DataStreamSource<String> ds = env.fromElements(words);
        //Transformations
        //对DataStream应用一个flatMap转换。对DataStream中的每一个元素都会调用FlatMapFunction接口的具体实现类。
        //flatMap方法可以返回任意个元素，当然也可以什么都不返回。
        SingleOutputStreamOperator<String> flatMap = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
                out.collect(value.toUpperCase());
            }
        });
        //sinks打印出信息
        //给DataStream添加一个Sinks
        flatMap.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value) throws Exception {
                System.out.println(value);
            }
        });
        //运行
        return env.execute("HelloWorld");
    }
}
