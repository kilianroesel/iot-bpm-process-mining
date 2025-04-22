package org.tum.bpm.sources;

import java.io.IOException;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.tum.bpm.configuration.MongoConfiguration;
import org.tum.bpm.schemas.pmining.DeviceOcelEvent;
import org.tum.bpm.serialization.OcelEventDebeziumDeserializationSchema;

public class OcelMongoSource {

    public static MongoConfiguration mongoConfiguration = MongoConfiguration.getConfiguration();

    public static MongoDBSource<DeviceOcelEvent> createOcelEventSource() throws IOException {
        MongoDBSource<DeviceOcelEvent> mongoSource = MongoDBSource.<DeviceOcelEvent>builder()
                .scheme(mongoConfiguration.getProperty("mongodb.scheme"))
                .hosts(mongoConfiguration.getProperty("mongodb.hosts"))
                .username(mongoConfiguration.getProperty("mongodb.userName"))
                .password(mongoConfiguration.getProperty("mongodb.password"))
                .connectionOptions(mongoConfiguration.getProperty("mongodb.connectionOptions"))
                .startupOptions(StartupOptions.initial())
                .batchSize(4096)
                .databaseList("bpm_ocel") // set captured database, support regex
                .collectionList("bpm_ocel.23005663")
                .deserializer(new OcelEventDebeziumDeserializationSchema())
                .build();
        return mongoSource;
    }

    public static WatermarkStrategy<DeviceOcelEvent> createWatermarkStrategy() {
        WatermarkStrategy<DeviceOcelEvent> watermarkStrategy = WatermarkStrategy
                // Handle data that is max 20 seconds out of order
                .<DeviceOcelEvent>forBoundedOutOfOrderness(Duration.ofMillis(5))
                .withTimestampAssigner((event, timestamp) -> event.getEvent().getTime().toEpochMilli())
                .withIdleness(Duration.ofSeconds(20));
        return watermarkStrategy;
    }
}