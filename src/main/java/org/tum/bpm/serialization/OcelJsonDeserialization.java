package org.tum.bpm.serialization;

import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.tum.bpm.schemas.ocel.OcelEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class OcelJsonDeserialization extends RichFlatMapFunction<String, OcelEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void flatMap(String line, Collector<OcelEvent> out) {
        try {
            // Parse the JSON array
            List<OcelEvent> items = objectMapper.readValue(line, new TypeReference<List<OcelEvent>>() {});
            if (items != null) {
                for (OcelEvent item : items) {
                    // Emit each item in the JSON array
                    out.collect(item);
                }
            }
        } catch (JsonProcessingException e) {
            // Handle invalid JSON gracefully (you might log or count errors here)
            System.out.println(e);
        }
    }
}
