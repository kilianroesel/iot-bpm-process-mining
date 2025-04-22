package org.tum.bpm.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.tum.bpm.schemas.ocel.FlatOcelEvent;
import org.tum.bpm.schemas.ocel.OcelRelationship;
import org.tum.bpm.schemas.pmining.DeviceOcelEvent;

/**
 * Replicates each event according to the number of its relationships.
 */
public class FlattenFunction extends RichFlatMapFunction<DeviceOcelEvent, FlatOcelEvent> {

    @Override
    public void flatMap(DeviceOcelEvent event, Collector<FlatOcelEvent> out) throws Exception {
        for (OcelRelationship relationship: event.getEvent().getRelationships()) {
            out.collect(new FlatOcelEvent(event.getEvent().getId(), event.getEvent().getType(), event.getEvent().getTime(), relationship, event.getEvent().getAttributes(), event.getDevice()));
        }
    }
    
}
