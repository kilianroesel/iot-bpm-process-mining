
package org.tum.bpm.jobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.tum.bpm.functions.HeuristicsMinerLossyCounting;
import org.tum.bpm.functions.FlattenFunction;
import org.tum.bpm.schemas.ocel.FlatOcelEvent;
import org.tum.bpm.schemas.ocel.OcelRelationship;
import org.tum.bpm.schemas.pmining.DeviceOcelEvent;
import org.tum.bpm.schemas.pmining.heuristicNet.HeuristicNet;
import org.tum.bpm.sinks.HeuristicNetMongoSink;
import org.tum.bpm.sinks.dynamicMongoSink.MetaDocument;
import org.tum.bpm.sources.OcelKafkaSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Applies a process mining algorithm on the event log for each object
 * 
 * @param args
 * @throws Exception
 */
public class ProcessMiningJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStream<OcelEvent> sourceStream = env
        // .fromSource(OcelFileSource.createOcelEventSource(),
        // OcelFileSource.createWatermarkStrategy(),
        // "Event Source");

        DataStream<DeviceOcelEvent> sourceStream = env
                .fromSource(OcelKafkaSource.createOcelEventSource(),
                        OcelKafkaSource.createWatermarkStrategy(),
                        "Event Source");

        

        // sourceStream.filter(event -> event.getDevice().equals("23005663"))
        // .filter(event -> event.getEventTime().toEpochMilli() <= 1742943600000L).keyBy(value -> "hi")
        // .process(new MovingAverageProcessFunction()).print();

        // DataStream<Alarm> alarmStream = env
        // .fromSource(AlarmKafkaSource.createAlarmEventSource(),
        // AlarmKafkaSource.createWatermarkStrategy(),
        // "Alarm Source");
        // alarmStream.print();

        DataStream<HeuristicNet> heuristicNet = sourceStream
                .flatMap(new FlattenFunction())
                .keyBy(event -> event.getProcessCase().getQualifier())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(40)))
                .process(new ProcessWindowFunction<FlatOcelEvent, FlatOcelEvent, String, TimeWindow>() {

                    @Override
                    public void process(String key,
                            ProcessWindowFunction<FlatOcelEvent, FlatOcelEvent, String, TimeWindow>.Context context,
                            Iterable<FlatOcelEvent> elements, org.apache.flink.util.Collector<FlatOcelEvent> out)
                            throws Exception {
                                                List<FlatOcelEvent> sorted = new ArrayList<>();
                        elements.forEach(sorted::add);
                        sorted.sort(Comparator.comparing(FlatOcelEvent::getTime));
                        sorted.forEach(out::collect);
                    }
                })
                .keyBy(event -> event.getProcessCase().getQualifier())
                .process(new HeuristicsMinerLossyCounting(1, 0.6, 0.6, 0));

        heuristicNet.map(new MapFunction<HeuristicNet, MetaDocument<HeuristicNet>>() {

            @Override
            public MetaDocument<HeuristicNet> map(HeuristicNet value) throws Exception {
                return new MetaDocument<HeuristicNet>(value.getDevice(), value);
            }
            
        }).sinkTo(HeuristicNetMongoSink.createHeuristicNetSink());

        // final FileSink<DeviceOcelEvent> sink = FileSink
        // .forRowFormat(new Path("output.json"), new
        // SimpleStringEncoder<DeviceOcelEvent>("UTF-8"))
        // .withRollingPolicy(
        // DefaultRollingPolicy.builder()
        // .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
        // .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
        // .withMaxPartSize(1024 * 1024 * 1024)
        // .build())
        // .build();
        // sourceStream.filter(event ->
        // event.getDevice().equals("23005663")).sinkTo(sink);

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
}
