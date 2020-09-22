package com.cqx.examples.kafka;

import com.cqx.examples.utils.SchemaUtil;
import com.cqx.examples.utils.SimpleClientConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.security.auth.login.Configuration;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Properties;

/**
 * KafkaSource
 *
 * @author chenqixu
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {
        //检查参数
        if (args.length != 1) {
            System.out.println("Args need topic_name , please check!");
            System.exit(-1);
        }
        String topic_name = args[0];
        final long checkDateStart = getTime("20190603110000");
        final long checkDateEnd = getTime("20190603110100");

        //创建一个 flink steam 程序的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //Kafka配置属性
        Properties properties = new Properties();
        //指定Kafka的Broker地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.1.8.200:9092");
        //指定消费组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flinkDemoGroup");
        //如果没有记录偏移量，第一次从最开始消费
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        Configuration.setConfiguration(new SimpleClientConfiguration("admin", "admin"));

        String schemaUrl = "http://10.1.8.203:19090/nl-edc-cct-sys-ms-dev/SchemaService/getSchema?t=";
        SchemaUtil schemaUtil = new SchemaUtil(schemaUrl);
        Schema schema = schemaUtil.getSchemaByTopic(topic_name);

        FlinkKafkaConsumer<GenericRecord> kafkaConsumerSource = new FlinkKafkaConsumer<>(topic_name,
                AvroDeserializationSchema.forGeneric(schema), properties);
        //强制从最开始消费
        kafkaConsumerSource.setStartFromEarliest();
        //通过addSource()方式，创建 Kafka DataStream
        DataStreamSource<GenericRecord> kafkaDataStream = env.addSource(kafkaConsumerSource);
        //过滤
        SingleOutputStreamOperator<GenericRecord> singleOutputStreamOperator = kafkaDataStream.filter(new FilterFunction<GenericRecord>() {
            @Override
            public boolean filter(GenericRecord genericRecord) throws Exception {
                int tac = (int) genericRecord.get("tac");
//                int cell_id = (int) genericRecord.get("cell_id");
                if (tac == 22927) return true;
                return false;
            }
        }).filter(new FilterFunction<GenericRecord>() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

            @Override
            public boolean filter(GenericRecord genericRecord) throws Exception {
                String procedure_start_time = genericRecord.get("procedure_start_time").toString();
                long time = sdf.parse(procedure_start_time).getTime();
                if (time >= checkDateStart && time < checkDateEnd) return true;
                return false;
            }
        });
        SingleOutputStreamOperator<String> singleOutputStreamOperatorMap = singleOutputStreamOperator.map(new MapFunction<GenericRecord, String>() {
            @Override
            public String map(GenericRecord genericRecord) throws Exception {
                return genericRecord.get("xdr_id").toString();
            }
        });
        //5秒滚动一次，TumblingWindow
        AllWindowedStream<String, TimeWindow> allWindowedStream = singleOutputStreamOperatorMap.timeWindowAll(Time.milliseconds(100));
        //sum by position & Sink输出
        allWindowedStream.apply(new AllWindowFunction<String, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<Object> collector) throws Exception {
                Iterator<String> iterator = iterable.iterator();
                int count = 0;
                while (iterator.hasNext()) {
                    iterator.next();
                    count++;
                }
                collector.collect(count);
            }
        }).print();
        //执行任务
        env.execute("KafkaSource");
    }

    private static Long getTime(String checkDatePoint) throws ParseException {
        return new SimpleDateFormat("yyyyMMddHHmmss").parse(checkDatePoint).getTime();
    }
}
