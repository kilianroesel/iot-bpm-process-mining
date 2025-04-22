package org.tum.bpm.serialization;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class ObjectToJsonFunction<T> implements MapFunction<T, String> {
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public String map(T value) throws Exception {
        return objectMapper.writeValueAsString(value);
    }
}
