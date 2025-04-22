package org.tum.bpm.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.tum.bpm.schemas.ocel.FlatOcelEvent;

public class CustomWindow extends KeyedProcessFunction<String, FlatOcelEvent, FlatOcelEvent> {

    private ListState<FlatOcelEvent> eventState;

    @Override
    public void open(Configuration parameters) {
        eventState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("events", FlatOcelEvent.class));
    }

    @Override
    public void processElement(FlatOcelEvent event,
            KeyedProcessFunction<String, FlatOcelEvent, FlatOcelEvent>.Context ctx,
            Collector<FlatOcelEvent> out) throws Exception {
        long eventTime = event.getTime().toEpochMilli();
        long currentWatermark = ctx.timerService().currentWatermark();
        if (eventTime < currentWatermark) {
            System.out.println("Late event detected: " + event);
        } else {
            eventState.add(event);
            ctx.timerService().registerEventTimeTimer(event.getTime().toEpochMilli());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FlatOcelEvent> out) throws Exception {
        List<FlatOcelEvent> events = new ArrayList<>();
        for (FlatOcelEvent e : eventState.get()) {
            events.add(e);
        }
        Collections.sort(events);
        for (FlatOcelEvent e : events) {
            out.collect(e);
        }
        eventState.clear();
    }
}
