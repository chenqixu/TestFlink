package com.cqx.examples.test;

import com.cqx.examples.utils.SchemaUtil;
import com.cqx.examples.utils.SimpleClientConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.util.Properties;

/**
 * FileToKafkaJob
 *
 * @author chenqixu
 */
public class FileToKafkaJob {

    private static final Logger logger = LoggerFactory.getLogger(FileToKafkaJob.class);
    private static final Schema schema1 = new Schema.Parser().parse("{\n" +
            "\"namespace\": \"com.newland\",\n" +
            "\"type\": \"record\",\n" +
            "\"name\": \"lte_http\",\n" +
            "\"fields\":[\n" +
            "{\"name\": \"length\", \"type\": [\"string\"]},\n" +
            "{\"name\": \"city\", \"type\": [\"string\"]},\n" +
            "{\"name\": \"interface\", \"type\": [\"string\"]},\n" +
            "{\"name\": \"xdr_id\", \"type\": [\"string\"] },\n" +
            "{\"name\": \"rat\", \"type\": [\"string\"]},\n" +
            "{\"name\": \"imsi\", \"type\": [\"string\"]},\n" +
            "{\"name\": \"imei\", \"type\": [\"string\"]},\n" +
            "{\"name\": \"msisdn\", \"type\": [\"string\"]},\n" +
            "{\"name\": \"procedure_type\", \"type\": [\"string\"]}\n" +
            "]\n" +
            "}");
    private static boolean isPrint = false;

    public static void main(String[] args) throws Exception {
        //检查参数
        if (args.length != 1) {
            System.out.println("Args need topic_name , please check!");
            System.exit(-1);
        }
        String topic_name = args[0];

        //创建一个 flink steam 程序的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> textFileSource = env.readTextFile(
                "d:\\tmp\\data\\dpi\\dpi_s1mme\\LTE_S1MME_028470789002_20190603110101.txt");

        String schemaUrl = "http://10.1.8.203:19090/nl-edc-cct-sys-ms-dev/SchemaService/getSchema?t=";
        SchemaUtil schemaUtil = new SchemaUtil(schemaUrl);
        Schema schema = schemaUtil.getSchemaByTopic(topic_name);

        SingleOutputStreamOperator<GenericRecord> outputStreamOperator = textFileSource.map(
                new KafkaMapFunction(schema));
        if (isPrint) {//打印到输出
            outputStreamOperator.print();
        } else {//入kafka
            //Kafka配置属性
            Properties properties = new Properties();
            //指定Kafka的Broker地址
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.1.8.200:9092");
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.mechanism", "PLAIN");
            Configuration.setConfiguration(new SimpleClientConfiguration("admin", "admin"));
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            FlinkKafkaProducer<GenericRecord> flinkKafkaProducer = new FlinkKafkaProducer<>(topic_name,
                    AvroSerializationSchema.forGeneric(schema), properties);
            outputStreamOperator.addSink(flinkKafkaProducer);
        }

        JobExecutionResult jobExecutionResult = env.execute("FileToKafkaJob");
        long counter_checkAvroInitialized = jobExecutionResult.getAccumulatorResult("counter_checkAvroInitialized");
        long counter_checkSchemaInitialized = jobExecutionResult.getAccumulatorResult("counter_checkSchemaInitialized");
        logger.info("counter_checkAvroInitialized：{}，counter_checkSchemaInitialized：{}", counter_checkAvroInitialized, counter_checkSchemaInitialized);
        System.out.println("counter_checkAvroInitialized：" + counter_checkAvroInitialized);
        System.out.println("counter_checkSchemaInitialized：" + counter_checkSchemaInitialized);
    }

}
