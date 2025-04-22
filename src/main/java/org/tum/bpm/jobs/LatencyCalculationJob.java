package org.tum.bpm.jobs;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.tum.bpm.schemas.pmining.DeviceOcelEvent;
import org.tum.bpm.sources.OcelKafkaSource;
import org.tum.bpm.functions.MovingAverageProcessFunction;

/**
 * Applies a process mining algorithm on the event log for each object
 * 
 * @param args
 * @throws Exception
 */
public class LatencyCalculationJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DeviceOcelEvent> sourceStream = env
                .fromSource(OcelKafkaSource.createOcelEventSource(),
                        OcelKafkaSource.createWatermarkStrategy(),
                        "Event Source");
        
        sourceStream
        .keyBy(value -> "key")
        .process(new MovingAverageProcessFunction()).print();

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
}
