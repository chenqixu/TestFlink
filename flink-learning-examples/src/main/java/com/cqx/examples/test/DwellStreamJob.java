package com.cqx.examples.test;

import com.cqx.examples.utils.SchemaUtil;
import com.cqx.examples.utils.SimpleClientConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.security.auth.login.Configuration;
import java.util.Iterator;
import java.util.Properties;

/**
 * dwell scene
 *
 * @author chenqixu
 */
public class DwellStreamJob {

    public static void main(String[] args) throws Exception {
        //检查参数
        if (args.length != 1) {
            System.out.println("Args need topic_name , please check!");
            System.exit(-1);
        }
        String topic_name = args[0];

        //创建一个 flink steam 程序的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
        /**
         * 启用Flink的checkpoint机制后，Flink Kafka Consumer在消费kafka消息的同时，
         * 会周期的并保持一致性的将offset写入checkpoint中。
         * 如果作业失败，Flink会将程序恢复到最新的checkpoint的状态，
         * 并从存储在checkpoint中的偏移量开始重新消费Kafka中的消息。
         *
         * 如果没有启用checkpoint机制，Kafka使用者将定期向Zookeeper提交偏移量
         */
//        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

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
        /**
         * Map<KafkaTopicPartition, Long> Long参数指定的offset位置
         * KafkaTopicPartition构造函数有两个参数，第一个为topic名字，第二个为分区数
         * 获取offset信息，可以用过Kafka自带的kafka-consumer-groups.sh脚本获取
         */
        HashedMap offsets = new HashedMap();
        offsets.put(new KafkaTopicPartition("maxwell_new", 0), 11111111L);
        offsets.put(new KafkaTopicPartition("maxwell_new", 1), 222222L);
        offsets.put(new KafkaTopicPartition("maxwell_new", 2), 33333333L);
        /**
         * Flink从topic中指定的offset开始，这个比较复杂，需要手动指定offset
         */
//        kafkaConsumerSource.setStartFromSpecificOffsets(offsets);
        /**
         * Flink从topic中指定的时间点开始消费，指定时间点之前的数据忽略
         */
//        kafkaConsumerSource.setStartFromTimestamp(1559801580000l);
        /**
         * Flink从topic中最新的数据开始消费
         */
//        kafkaConsumerSource.setStartFromLatest();
        /**
         * Flink从topic中指定的group上次消费的位置开始消费，所以必须配置group.id参数
         * 这也是默认的方式
         */
//        kafkaConsumerSource.setStartFromGroupOffsets();
        /**
         * Flink从topic中最初的数据开始消费
         */
        kafkaConsumerSource.setStartFromEarliest();
        //通过addSource()方式，创建 Kafka DataStream
        DataStreamSource<GenericRecord> source = env.addSource(kafkaConsumerSource);

        //转换: 按指定的Key对数据重分区，将相同Key的数据分到同一个分区
        KeyedStream<GenericRecord, String> stream1 = source.keyBy(new KeySelector<GenericRecord, String>() {
            @Override
            public String getKey(GenericRecord genericRecord) throws Exception {
                //手机号码取模，分3个分区
                long msisdn = Long.valueOf(genericRecord.get("msisdn").toString());
                long mod = msisdn % 3;
                return String.valueOf(mod);
            }
        });
        //分流
//        final OutputTag<GenericRecord> outputTag = new OutputTag<>("tag1");
//        SingleOutputStreamOperator<GenericRecord> stream2 = stream1.process(new KeyedProcessFunction<String, GenericRecord, GenericRecord>() {
//            @Override
//            public void processElement(GenericRecord genericRecord, Context context, Collector<GenericRecord> collector) throws Exception {
//                collector.collect(genericRecord);
//            }
//        });
        //=====================流1==========================
//        SingleOutputStreamOperator<GenericRecord> streamOperator1 = stream1.filter(new FilterFunction<GenericRecord>() {
//            //基站过滤
//            @Override
//            public boolean filter(GenericRecord genericRecord) throws Exception {
//                return false;
//            }
//        }).filter(new FilterFunction<GenericRecord>() {
//            //时间过滤
//            @Override
//            public boolean filter(GenericRecord genericRecord) throws Exception {
//                return false;
//            }
//        });
        stream1.timeWindow(Time.milliseconds(1000)).apply(new WindowFunction<GenericRecord, Object, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow timeWindow, Iterable<GenericRecord> iterable, Collector<Object> collector) throws Exception {
                Iterator<GenericRecord> iterator = iterable.iterator();
                int count = 0;
                while (iterator.hasNext()) {
                    iterator.next();
                    count++;
                }
                collector.collect(count);
            }
        }).print();

        //=====================流2==========================
//        stream2.filter(new FilterFunction<GenericRecord>() {
//            @Override
//            public boolean filter(GenericRecord genericRecord) throws Exception {
//                return false;
//            }
//        });
//        stream2.timeWindowAll(Time.milliseconds(100)).apply(new AllWindowFunction<GenericRecord, Object, TimeWindow>() {
//            @Override
//            public void apply(TimeWindow timeWindow, Iterable<GenericRecord> iterable, Collector<Object> collector) throws Exception {
//                Iterator<GenericRecord> iterator = iterable.iterator();
//                int count = 0;
//                while (iterator.hasNext()) {
//                    iterator.next();
//                    count++;
//                }
//                collector.collect(count);
//            }
//        }).print();

        //运行
        env.execute("DwellStreamJob");
    }
}
