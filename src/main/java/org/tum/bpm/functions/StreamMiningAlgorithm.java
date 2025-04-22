package org.tum.bpm.functions;

import java.io.IOException;
import java.time.Instant;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.tum.bpm.schemas.ocel.FlatOcelEvent;
import org.tum.bpm.schemas.pmining.ProcessMiningResponse;

/**
 * This abstract class defines the root of the mining algorithms hierarchy. It
 * is a {@link RichFlatMapFunction} of elements with type {@link BEvent} that is
 * capable of producing responses of type {@link ProcessMiningResponse}.
 * 
 * <p>
 * Since this map is actually "rich" this means that classes that extends this
 * one can have access to the state of the operator and use it in a distributed
 * fashion. Additionally, being this map a "flat" it might return 0 or 1 results
 * for each event being consumed.
 * 
 * @author Andrea Burattin
 */
public abstract class StreamMiningAlgorithm<T extends ProcessMiningResponse> extends KeyedProcessFunction<String, FlatOcelEvent, T> {

	private transient ValueState<Long> processedEvents;
	private transient ValueState<Instant> startTime;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.processedEvents = getRuntimeContext().getState(new ValueStateDescriptor<>("processedEvents", Long.class));
		this.startTime = getRuntimeContext().getState(new ValueStateDescriptor<>("startTime", Instant.class));
	}

	@Override
	public void processElement(FlatOcelEvent event, KeyedProcessFunction<String, FlatOcelEvent, T>.Context ctx,
			Collector<T> out) throws Exception {

		T latestResponse = process(event, ctx.getCurrentKey());
		if (latestResponse != null) {
			out.collect(latestResponse);
		}
	}

	/**
	 * This abstract method is what each derive class is expected to implement.
	 * The argument of the method is the new observation and the returned value
	 * is the result of the mining.
	 * 
	 * <p>
	 * If this method returns value <code>null</code>, then the value is not
	 * moved forward into the pipeline.
	 * 
	 * @param event the new event being observed
	 * @return the result of the mining of the event, or <code>null</code> if
	 *         nothing should go through the rest of the pipeline
	 */
	public abstract T ingest(FlatOcelEvent event) throws Exception;

	/**
	 * Returns the total number of events processed so far
	 * 
	 * @return the total number of events processed so far
	 */
	public long getProcessedEvents() {
		try {
			if (processedEvents == null || processedEvents.value() == null) {
				return 0;
			}
			return processedEvents.value().longValue();
		} catch (IOException e) {
			// this exception would mean that there are serialization issues
		}
		return 0;
	}

	/**
	 * Returns the time of the first processed event
	 * 
	 * @return the time of the first processed event
	 */
	public Instant getStartTime() {
		try {
			if (this.startTime == null || this.startTime.value() == null) {
				return Instant.now();
			}
			return startTime.value();
		} catch (IOException e) {
			// this exception would mean that there are serialization issues
		}
		return Instant.now();
	}

	/*
	 * The internal processor in charge of updating the internal status of the
	 * map.
	 */
	protected T process(FlatOcelEvent event, String key) throws Exception {
		try {
			long value = 1;
			if (this.processedEvents != null) {
				if (this.processedEvents.value() != null) {
					value = processedEvents.value() + 1;
				}
				processedEvents.update(value);
			}
			if (this.startTime != null) {
				if (this.startTime.value() == null) {
					this.startTime.update(event.getTime());
				}
			}
		} catch (IOException e) {
			// this exception would mean that there are serialization issues
		}
		T response = ingest(event);
		if (response != null) {
			response.setStartTime(this.getStartTime());
			response.setEndTime(event.getTime());
			response.setProcessedEvents(this.getProcessedEvents());
			response.setObjectView(key);
			response.setDevice(event.getDevice());
		}
		return response;
	}
}