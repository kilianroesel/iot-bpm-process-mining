package org.tum.bpm.sources;

import java.io.IOException;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.tum.bpm.configuration.KafkaConfiguration;
import org.tum.bpm.schemas.pmining.Alarm;

public class AlarmKafkaSource {

    public static KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getConfiguration();

    public static KafkaSource<Alarm> createAlarmEventSource() throws IOException {
        KafkaSource<Alarm> source = KafkaSource.<Alarm>builder()
                .setProperties(kafkaConfiguration.getProperties())
                .setTopics("eh-bpm-alarms-prod")
                .setStartingOffsets(OffsetsInitializer.timestamp(1742860800000L))
                .setValueOnlyDeserializer(new JsonDeserializationSchema<Alarm>(Alarm.class))
                .build();

        return source;
    }

    public static WatermarkStrategy<Alarm> createWatermarkStrategy() {
        WatermarkStrategy<Alarm> watermarkStrategy = WatermarkStrategy
                // Handle data that is max 20 seconds out of order
                .<Alarm>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // Assign event Time Timestamps to each measurement
                .withTimestampAssigner((event, timestamp) -> event.getAlarmTime().toEpochMilli())
                // If a source does not generate events for 60 seconds it is considered idle and the watermark progresses
                .withIdleness(Duration.ofSeconds(10));
        return watermarkStrategy;
    }
}
