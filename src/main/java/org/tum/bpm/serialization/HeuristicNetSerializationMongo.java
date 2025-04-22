package org.tum.bpm.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.bson.BsonDocument;
import org.tum.bpm.schemas.pmining.heuristicNet.HeuristicNet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

public class HeuristicNetSerializationMongo implements MongoSerializationSchema<HeuristicNet> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(
            SerializationSchema.InitializationContext initializationContext,
            MongoSinkContext sinkContext,
            MongoWriteOptions sinkConfiguration)
            throws Exception {
        this.objectMapper.registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public WriteModel<BsonDocument> serialize(HeuristicNet element, MongoSinkContext sinkContext) {
        try {
            String json = objectMapper.writeValueAsString(element);
            return new ReplaceOneModel<>(Filters.eq("objectView", element.getObjectView()), BsonDocument.parse(json), new ReplaceOptions().upsert(true));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing object to JSON", e);
        }
    }
}