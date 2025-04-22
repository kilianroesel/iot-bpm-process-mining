package org.tum.bpm.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.tum.bpm.schemas.ocel.FlatOcelEvent;
import org.tum.bpm.schemas.pmining.DirectlyFollowsGraph;
import org.tum.bpm.schemas.pmining.helper.Relation;

public class DFGDiscovery extends StreamMiningAlgorithm<DirectlyFollowsGraph> {

	// Stores the latest activity of each case
	private transient MapState<String, String> latestActivityInCase;
	// Stores the frequency by which an activity follows another activity
	private transient MapState<Relation, Long> directlyFollowsRelationCount;
	// Stores the starting and end activities respectively
	private transient MapState<String, Long> startingActivityCount;

	private MapStateDescriptor<String, String> latestActivityStateDescriptor = new MapStateDescriptor<>(
			"latestActivityState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
	private MapStateDescriptor<Relation, Long> directlyFollowsStateDescriptor = new MapStateDescriptor<>(
			"directlyFollowsRelationCount",
			TypeInformation.of(new TypeHint<Relation>() {
			}),
			BasicTypeInfo.LONG_TYPE_INFO);
	private MapStateDescriptor<String, Long> startingActivitiesStateDescriptor = new MapStateDescriptor<>(
			"startingActivityCount",
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO);

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.latestActivityInCase = getRuntimeContext().getMapState(latestActivityStateDescriptor);
		this.directlyFollowsRelationCount = getRuntimeContext().getMapState(directlyFollowsStateDescriptor);
		this.startingActivityCount = getRuntimeContext().getMapState(startingActivitiesStateDescriptor);
	}

	@Override
	public DirectlyFollowsGraph ingest(FlatOcelEvent event) throws Exception {
		String caseId = event.getProcessCase().getObjectId();
		String activityName = event.getType();
		String latestActivityInCase = this.latestActivityInCase.get(caseId);
		if (latestActivityInCase == null) {
			this.incrementStartingActivities(activityName);
		} else {
			this.incrementNumberOfDirectlyFollows(latestActivityInCase, activityName);
		}
		this.latestActivityInCase.put(caseId, activityName);
		return this.createDfg();
	}

	private DirectlyFollowsGraph createDfg() throws Exception {
		HashMap<Relation, Long> relations = new HashMap<>();
        for (Map.Entry<Relation,Long> entry : this.directlyFollowsRelationCount.entries()) {
            relations.put(entry.getKey(), entry.getValue());
        }

		HashMap<String, Long> startingActivities = new HashMap<>();
		for (Map.Entry<String,Long> entry : this.startingActivityCount.entries()) {
            startingActivities.put(entry.getKey(), entry.getValue());
        }

		HashMap<String, Long> endingActivities = new HashMap<>();
		for (String activity : this.latestActivityInCase.values()) {
			Long numberOfEndingActivities = endingActivities.get(activity);
			if (numberOfEndingActivities == null) {
				numberOfEndingActivities = 0L;
			} else {
				numberOfEndingActivities = numberOfEndingActivities + 1L;
			}
            endingActivities.put(activity, numberOfEndingActivities);
        }
		return new DirectlyFollowsGraph(relations, startingActivities, endingActivities);	
	}

	private void incrementNumberOfDirectlyFollows(String activity1, String activity2) {
		Relation relation = new Relation(activity1, activity2);

		try {
			Long numberOfDirectlyFollows = this.directlyFollowsRelationCount.get(relation);
			if (numberOfDirectlyFollows == null) {
				numberOfDirectlyFollows = 1L;
			} else {
				numberOfDirectlyFollows = numberOfDirectlyFollows + 1;
			}
			this.directlyFollowsRelationCount.put(relation, numberOfDirectlyFollows);
		} catch (Exception e) {
			// this exception would mean that there are serialization issues
		}
	}

	private void incrementStartingActivities(String activity) {
		try {
			Long numberOfStartingActivities = this.startingActivityCount.get(activity);
			if (numberOfStartingActivities == null) {
				numberOfStartingActivities = 1L;
			} else {
				numberOfStartingActivities = numberOfStartingActivities + 1;
			}
			this.startingActivityCount.put(activity, numberOfStartingActivities);
		} catch (Exception e) {
			// this exception would mean that there are serialization issues
		}
	}
}