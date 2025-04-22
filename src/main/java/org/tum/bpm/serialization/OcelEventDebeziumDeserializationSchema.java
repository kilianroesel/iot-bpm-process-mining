package org.tum.bpm.serialization;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.tum.bpm.schemas.debezium.DebeziumChangeStreamMessage;
import org.tum.bpm.schemas.ocel.OcelEvent;
import org.tum.bpm.schemas.pmining.DeviceOcelEvent;

import java.util.HashMap;

/**
 * A JSON format implementation of {@link DebeziumDeserializationSchema} which
 * deserializes the
 * received {@link SourceRecord} to JSON String.
 */
public class OcelEventDebeziumDeserializationSchema implements DebeziumDeserializationSchema<DeviceOcelEvent> {

    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;
    private transient ObjectMapper objectMapper;

    /**
     * Configuration whether to enable
     * {@link JsonConverterConfig#SCHEMAS_ENABLE_CONFIG} to include
     * schema in messages.
     */
    private final Boolean includeSchema;

    public OcelEventDebeziumDeserializationSchema() {
        this(false);
    }

    public OcelEventDebeziumDeserializationSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<DeviceOcelEvent> out) throws Exception {
        if (jsonConverter == null) {
            initializeJsonConverter();
        }
        if (this.objectMapper == null) {
            initializeObjectMapper();
        }
        byte[] bytes = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        DebeziumChangeStreamMessage changeStreamMessage = this.objectMapper.readValue(new String(bytes), DebeziumChangeStreamMessage.class);
        if (changeStreamMessage.getOperationType().equals("insert")
                || changeStreamMessage.getOperationType().equals("update")) {
            JsonNode jsonNode = objectMapper.readTree(changeStreamMessage.getFullDocument());

            OcelEvent ocelEvent = objectMapper.treeToValue(jsonNode, OcelEvent.class);
            DeviceOcelEvent deviceOcelWrapper = new DeviceOcelEvent(ocelEvent, changeStreamMessage.getNamespace().getCollection());
            out.collect(deviceOcelWrapper);
        }
    }

    /** Initialize {@link JsonConverter} with given configs. */
    private void initializeJsonConverter() {
        jsonConverter = new JsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
        jsonConverter.configure(configs);
    }

    
    /** Initialize {@link JsonConverter} with given configs. */
    private void initializeObjectMapper() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public TypeInformation<DeviceOcelEvent> getProducedType() {
        return TypeInformation.of(new TypeHint<DeviceOcelEvent>() {});
    }
}
