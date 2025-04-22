package org.tum.bpm.sources;

import java.io.IOException;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.tum.bpm.configuration.KafkaConfiguration;
import org.tum.bpm.schemas.pmining.DeviceOcelEvent;

public class OcelKafkaSource {

    public static KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getConfiguration();

    public static KafkaSource<DeviceOcelEvent> createOcelEventSource() throws IOException {
        KafkaSource<DeviceOcelEvent> source = KafkaSource.<DeviceOcelEvent>builder()
                .setProperties(kafkaConfiguration.getProperties())
                .setTopics("eh-bpm-ocelevents-23005663")
                .setStartingOffsets(OffsetsInitializer.timestamp(										1743327710000L))
                .setValueOnlyDeserializer(new JsonDeserializationSchema<DeviceOcelEvent>(DeviceOcelEvent.class))
                .build();

        return source;
    }

    public static WatermarkStrategy<DeviceOcelEvent> createWatermarkStrategy() {
        WatermarkStrategy<DeviceOcelEvent> watermarkStrategy = WatermarkStrategy
                // Handle data that is max 20 seconds out of order
                .<DeviceOcelEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // Assign event Time Timestamps to each measurement
                .withTimestampAssigner((event, timestamp) -> event.getEvent().getTime().toEpochMilli())
                // If a source does not generate events for 60 seconds it is considered idle and the watermark progresses
                .withIdleness(Duration.ofSeconds(10));
        return watermarkStrategy;
    }
}
