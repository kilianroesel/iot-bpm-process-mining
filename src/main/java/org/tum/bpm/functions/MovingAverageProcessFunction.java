package org.tum.bpm.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.tum.bpm.schemas.pmining.Averages;
import org.tum.bpm.schemas.pmining.DeviceOcelEvent;


public class MovingAverageProcessFunction extends KeyedProcessFunction<String, DeviceOcelEvent, Averages> {

    private transient ValueState<Long> upstreamSumState;
    private transient ValueState<Long> scopingSumState;
    private transient ValueState<Long> abstractionSumState;
    private transient ValueState<Long> enrichmentSumState;
    private transient ValueState<Long> correlationSumState;
    private transient ValueState<Long> sinkSumState;
    private transient ValueState<Long> totalSumState;



    private transient ValueState<Long> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        upstreamSumState = getRuntimeContext().getState(new ValueStateDescriptor<>("upstreamSumState", Long.class));
        scopingSumState = getRuntimeContext().getState(new ValueStateDescriptor<>("scopingSumState", Long.class));
        abstractionSumState = getRuntimeContext().getState(new ValueStateDescriptor<>("abstractionSumState", Long.class));
        enrichmentSumState = getRuntimeContext().getState(new ValueStateDescriptor<>("enrichmentSumState", Long.class));
        correlationSumState = getRuntimeContext().getState(new ValueStateDescriptor<>("correlationSumState", Long.class));
        sinkSumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sinkSumState", Long.class));
        totalSumState = getRuntimeContext().getState(new ValueStateDescriptor<>("totalSumState", Long.class));


        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Long.class));

    }

    @Override
    public void processElement(DeviceOcelEvent value, Context ctx, Collector<Averages> out)
            throws Exception {
        // Update sum and count
        Long upstreamSum = upstreamSumState.value() != null ? upstreamSumState.value() : 0L;
        Long scopingSum = scopingSumState.value() != null ? scopingSumState.value() : 0L;
        Long abstractionSum = abstractionSumState.value() != null ? abstractionSumState.value() : 0L;
        Long enrichmentSum = enrichmentSumState.value() != null ? enrichmentSumState.value() : 0L;
        Long correlationSum = correlationSumState.value() != null ? correlationSumState.value() : 0L;
        Long sinkSum = sinkSumState.value() != null ? sinkSumState.value() : 0L;
        Long totalSum = totalSumState.value() != null ? totalSumState.value() : 0L;

        Long currentCount = countState.value() != null ? countState.value() : 0L;

        // Long eventTime = value.getEventTime().toEpochMilli();
        Long sendTimestamp = value.getSendTime().toEpochMilli();
        Long ingestionTimestamp = value.getIngestionTime().toEpochMilli();
        Long scopingTimestamp = value.getScopeTime().toEpochMilli();
        Long sinkTimestamp = value.getSinkTime().toEpochMilli();
        Long abstractionTimestamp = value.getAbstractionTime().toEpochMilli();
        Long enrichmentTimestamp = value.getEnrichmentTime().toEpochMilli();
        Long correlationTimestamp = value.getCorrelationTime().toEpochMilli();

        upstreamSum += ingestionTimestamp - sendTimestamp;
        upstreamSumState.update(upstreamSum);
        scopingSum += scopingTimestamp - ingestionTimestamp;
        scopingSumState.update(scopingSum);
        abstractionSum += abstractionTimestamp - scopingTimestamp;
        abstractionSumState.update(abstractionSum);
        enrichmentSum += enrichmentTimestamp - abstractionTimestamp;
        enrichmentSumState.update(enrichmentSum);
        correlationSum += correlationTimestamp - enrichmentTimestamp;
        correlationSumState.update(correlationSum);
        sinkSum += sinkTimestamp - correlationTimestamp;
        sinkSumState.update(sinkSum);
        totalSum += sinkTimestamp - ingestionTimestamp;
        totalSumState.update(totalSum);

        currentCount += 1;
        countState.update(currentCount);

        // Calculate and emit the moving average
        double avgUpstream = (double) upstreamSum / currentCount;
        double avgScoping = (double) scopingSum / currentCount;
        double avgAbstraction = (double) abstractionSum / currentCount;
        double avgEnrichment = (double) enrichmentSum / currentCount;
        double avgCorrelation = (double) correlationSum / currentCount;
        double avgTotalSum = (double) totalSum / currentCount;
        double avgSink = (double) sinkSum / currentCount;

        out.collect(new Averages(avgUpstream, avgScoping, avgAbstraction, avgEnrichment, avgCorrelation, avgSink, avgTotalSum, currentCount));
    }

    @Override
    public void close() throws Exception {

        countState.clear();
    }
}