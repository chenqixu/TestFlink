package com.cqx.examples.test;

import com.cqx.examples.utils.parser.StreamParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.AvroSerializationSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaMapFunction
 *
 * @author chenqixu
 */
public class KafkaMapFunction extends RichMapFunction<String, GenericRecord> {

    //    private GenericRecordUtil genericRecordUtil = new GenericRecordUtil();
    private final String[] sourceFields = "length,city,interface,xdr_id,rat,imsi,imei,msisdn,procedure_type,subprocedure_type,procedure_start_time,procedure_delay_time,procedure_end_time,procedure_status,cause,nas_cause,s1ap_cause1,s1ap_cause2,keyword,enb_ue_s1ap_id,mme_ue_s1ap_id,old_mme_group_id,old_mme_code,m_tmsi,mcc,mnc,lac,tmsi,user_ipv4,user_ipv6,machine_ip_add_type,mme_ip_add,enb_ip_add,mme_port,enb_port,tac,cell_id,other_tac,other_eci,mac,req_count,res_count,apn,eps_bearer_number,bearer_id1,bearer_type1,bearer_qci1,bearer_status1,bearer_enb_gtp_teid1,bearer_sgw_gtp_teid1,bearer_id2,bearer_type2,bearer_qci2,bearer_status2,bearer_enb_gtp_teid2,bearer_sgw_gtp_teid2,bearer_id3,bearer_type3,bearer_qci3,bearer_status3,bearer_enb_gtp_teid3,bearer_sgw_gtp_teid3,bearer_id4,bearer_type4,bearer_qci4,bearer_status4,bearer_enb_gtp_teid4,bearer_sgw_gtp_teid4,bearer_id5,bearer_type5,bearer_qci5,bearer_status5,bearer_enb_gtp_teid5,bearer_sgw_gtp_teid5,bearer_id6,bearer_type6,bearer_qci6,bearer_status6,bearer_enb_gtp_teid6,bearer_sgw_gtp_teid6,bearer_id7,bearer_type7,bearer_qci7,bearer_status7,bearer_enb_gtp_teid7,bearer_sgw_gtp_teid7,bearer_id8,bearer_type8,bearer_qci8,bearer_status8,bearer_enb_gtp_teid8,bearer_sgw_gtp_teid8,bearer_id9,bearer_type9,bearer_qci9,bearer_status9,bearer_enb_gtp_teid9,bearer_sgw_gtp_teid9,bearer_id10,bearer_type10,bearer_qci10,bearer_status10,bearer_enb_gtp_teid10,bearer_sgw_gtp_teid10,bearer_id11,bearer_type11,bearer_qci11,bearer_status11,bearer_enb_gtp_teid11,bearer_sgw_gtp_teid11,bearer_id12,bearer_type12,bearer_qci12,bearer_status12,bearer_enb_gtp_teid12,bearer_sgw_gtp_teid12,bearer_id13,bearer_type13,bearer_qci13,bearer_status13,bearer_enb_gtp_teid13,bearer_sgw_gtp_teid13,bearer_id14,bearer_type14,bearer_qci14,bearer_status14,bearer_enb_gtp_teid14,bearer_sgw_gtp_teid14,bearer_id15,bearer_type15,bearer_qci15,bearer_status15,bearer_enb_gtp_teid15,bearer_sgw_gtp_teid15,s_year,s_month,s_day,s_hour,s_minute,request_cause,old_mme_group_id_1,paging_type,keyword_2,keyword_3,keyword_4,old_mme_code_1,old_m_tmsi,bearer_1_request_cause,bearer_1_failure_cause,bearer_2_request_cause,bearer_2_failure_cause,bearer_3_request_cause,bearer_3_failure_cause,bearer_4_request_cause,bearer_4_failure_cause,bearer_5_request_cause,bearer_5_failure_cause,bearer_6_request_cause,bearer_6_failure_cause,bearer_7_request_cause,bearer_7_failure_cause,bearer_8_request_cause,bearer_8_failure_cause,bearer_9_request_cause,bearer_9_failure_cause,bearer_10_request_cause,bearer_10_failure_cause,bearer_11_request_cause,bearer_11_failure_cause,bearer_12_request_cause,bearer_12_failure_cause,bearer_13_request_cause,bearer_13_failure_cause,bearer_14_request_cause,bearer_14_failure_cause,bearer_15_request_cause,bearer_15_failure_cause,reserve_1,reserve_2,reserve_3,old_tac,old_eci".split(",", -1);
    private final String[] ruleFields = ",,,,,,,parserSubstrFirst-86,,,parserTimestamp-yyyyMMddHHmmss,,parserTimestamp-yyyyMMddHHmmss,,,,,,,,,,,,,,,,,,,,,,,parserConvertHex-10,parserConvertHex-10,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,".split(",", -1);
    private final String[] outFields = "city,xdr_id,imsi,imei,msisdn,procedure_type,subprocedure_type,procedure_start_time,procedure_delay_time,procedure_end_time,procedure_status,old_mme_group_id,old_mme_code,lac,tac,cell_id,other_tac,other_eci,home_code,msisdn_home_code,old_mme_group_id_1,old_mme_code_1,old_m_tmsi,old_tac,old_eci,cause,keyword,mme_ue_s1ap_id,request_cause,keyword_2,keyword_3,keyword_4,bearer_qci1,bearer_status1,bearer_qci2,bearer_status2,bearer_qci3,bearer_status3".split(",", -1);
    //    private static final Logger logger = LoggerFactory.getLogger(KafkaMapFunction.class);
    private transient Schema schema;
    private String schemaString;
    private AvroSerializationSchema<GenericRecord> avroSerializationSchema;
    private LongCounter counter_checkAvroInitialized = new LongCounter();
    private LongCounter counter_checkSchemaInitialized = new LongCounter();

    KafkaMapFunction(Schema schema) {
        this.schema = schema;
        this.schemaString = schema.toString();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("counter_checkAvroInitialized", counter_checkAvroInitialized);
        getRuntimeContext().addAccumulator("counter_checkSchemaInitialized", counter_checkSchemaInitialized);
    }

    private void checkAvroInitialized() {
        checkSchemaInitialized();
        if (avroSerializationSchema == null) {
            this.avroSerializationSchema = AvroSerializationSchema.forGeneric(this.schema);
            counter_checkAvroInitialized.add(1);
        }
    }

    private void checkSchemaInitialized() {
        if (this.schema == null) {
            this.schema = new Schema.Parser().parse(this.schemaString);
            counter_checkSchemaInitialized.add(1);
        }
    }

    @Override
    public GenericRecord map(String value) throws Exception {
//        checkAvroInitialized();
        checkSchemaInitialized();
//        String[] value_arr = value.split("\\|", -1);
//        Map<String, String> values = new HashMap<>();
        GenericRecord genericRecord = new GenericData.Record(this.schema);
//        genericRecord.put("length", value_arr[0]);
//        genericRecord.put("city", value_arr[1]);
//        genericRecord.put("interface", value_arr[2]);
//        genericRecord.put("xdr_id", value_arr[3]);
//        genericRecord.put("rat", value_arr[4]);
//        genericRecord.put("imsi", value_arr[5]);
//        genericRecord.put("imei", value_arr[6]);
//        genericRecord.put("msisdn", value_arr[7]);
//        genericRecord.put("procedure_type", value_arr[8]);

//        return avroSerializationSchema.serialize(genericRecord);
//        return new byte[]{-1, 0, 1};
//        return genericRecordUtil.genericRecord(this.schema, values);

        Map<String, String> sourceMap = sourceToMap(value);
        if (sourceMap.size() > 0) {
            for (String outKey : outFields) {
                genericRecord.put(outKey, sourceMap.get(outKey));
            }
        }
        return genericRecord;
    }


    /**
     * 原字段转换成map
     *
     * @param value
     * @return
     */
    private Map<String, String> sourceToMap(String value) {
        Map<String, String> sourceMap = new HashMap<>();
        try {
            String[] value_arr = value.split("\\|", -1);
            if (value_arr.length == sourceFields.length) {
                for (int i = 0; i < sourceFields.length; i++) {
                    //按照规则进行处理
                    String result = StreamParserUtil.parser(ruleFields[i], value_arr[i]);
                    //填入处理后的内容
                    sourceMap.put(sourceFields[i], result);
                }
            }
        } catch (Exception e) {
            //规则转换异常就不要了
        }
        return sourceMap;
    }
}
