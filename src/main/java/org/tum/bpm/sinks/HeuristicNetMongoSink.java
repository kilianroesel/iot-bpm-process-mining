package org.tum.bpm.sinks;

import java.io.IOException;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.tum.bpm.schemas.pmining.heuristicNet.HeuristicNet;
import org.tum.bpm.serialization.HeuristicNetSerializationMongo;
import org.tum.bpm.sinks.dynamicMongoSink.DynamicMongoSink;
import org.tum.bpm.configuration.MongoConfiguration;

public class HeuristicNetMongoSink {

    public static MongoConfiguration mongoConfiguration = MongoConfiguration.getConfiguration();

    public static DynamicMongoSink<HeuristicNet> createHeuristicNetSink() throws IOException {
        String uri = mongoConfiguration.getProperty("mongodb.scheme") + "://"
                + mongoConfiguration.getProperty("mongodb.userName") + ":"
                + mongoConfiguration.getProperty("mongodb.password") + "@"
                + mongoConfiguration.getProperty("mongodb.hosts");
        DynamicMongoSink<HeuristicNet> sink = DynamicMongoSink.<HeuristicNet>builder()
                .setUri(uri)
                .setDatabase("heuristic_net")
                .setCollection("test") // Collection must not be null, but is not used by DynamicMongoSink
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema(new HeuristicNetSerializationMongo())
                .build();

        return sink;
    }
}