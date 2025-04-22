package org.tum.bpm.functions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class LateEventFilterFunction<T> extends ProcessFunction<T, T> {
    
    public LateEventFilterFunction(java.util.function.ToLongFunction<T> timestampExtractor) {
    }

    @Override
    public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {
        ctx.timestamp();
        ctx.timerService().currentWatermark();
    }
}

