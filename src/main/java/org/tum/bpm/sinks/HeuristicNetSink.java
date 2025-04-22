package org.tum.bpm.sinks;

import java.io.IOException;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.tum.bpm.configuration.KafkaConfiguration;
import org.tum.bpm.schemas.pmining.heuristicNet.HeuristicNet;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class HeuristicNetSink {

    private static final String HEURISTIC_NET_TOPIC = "eh-bpm-heuristicnet-prod";
    public static KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getConfiguration();

    public static KafkaSink<HeuristicNet> createHeuristicNetSink() throws IOException {

        KafkaSink<HeuristicNet> sink = KafkaSink.<HeuristicNet>builder()
                .setBootstrapServers(kafkaConfiguration.getProperty("bootstrap.servers"))
                .setKafkaProducerConfig(kafkaConfiguration.getProperties())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(HEURISTIC_NET_TOPIC)
                        .setValueSerializationSchema(new JsonSerializationSchema<HeuristicNet>(
                                () -> new ObjectMapper().registerModule(new JavaTimeModule())))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        return sink;
    }
}
