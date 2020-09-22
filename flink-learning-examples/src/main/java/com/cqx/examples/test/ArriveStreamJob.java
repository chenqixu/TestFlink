package com.cqx.examples.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * arrive scene
 *
 * @author chenqixu
 */
public class ArriveStreamJob {

    private static final Logger logger = LoggerFactory.getLogger(ArriveStreamJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                logger.info(value);
            }
        });
        //运行
        env.execute("ArriveStreamJob");
    }
}
