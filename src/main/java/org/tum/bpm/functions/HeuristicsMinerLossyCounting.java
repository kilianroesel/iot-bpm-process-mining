package org.tum.bpm.functions;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.tum.bpm.schemas.ocel.FlatOcelEvent;
import org.tum.bpm.schemas.pmining.helper.CaseObservation;
import org.tum.bpm.schemas.pmining.helper.DirectFollowsObservation;
import org.tum.bpm.schemas.pmining.helper.Observation;
import org.tum.bpm.schemas.pmining.helper.Relation;
import org.tum.bpm.schemas.pmining.helper.TripleRelation;
import org.tum.bpm.schemas.pmining.heuristicNet.HeuristicNet;
import org.tum.bpm.schemas.pmining.heuristicNet.HeuristicNetBuilder;

/**
 * Implemented according to Andrea Buttin
 */
public class HeuristicsMinerLossyCounting extends StreamMiningAlgorithm<HeuristicNet> {

	// Stores the count and maximum error for each observed activity
	private transient MapState<String, Observation> activityObservations;
	// Stores the latest two activities, count, and maximum error for each observed case
	private transient MapState<String, CaseObservation> caseObservations;
	// Stores the count and maximum error for directFollows relations
	private transient MapState<Relation, DirectFollowsObservation> directlyFollowsObservations;
	// Stores the count and maximum error for tripleFollows relations
	private transient MapState<TripleRelation, Observation> tripleFollowsObservations;
	// Stores startActivites, the end activities do not need to be stored because
	// they are calculated from the caseObservations
	private transient ListState<String> startActivites;

	private MapStateDescriptor<String, Observation> activityObservationsStateDescriptor = new MapStateDescriptor<>(
			"activityObservationsState", BasicTypeInfo.STRING_TYPE_INFO,
			TypeInformation.of(new TypeHint<Observation>() {
			}));
	private MapStateDescriptor<String, CaseObservation> caseObservationsStateDescriptor = new MapStateDescriptor<>(
			"caseObservationsState",
			BasicTypeInfo.STRING_TYPE_INFO,
			TypeInformation.of(new TypeHint<CaseObservation>() {
			}));
	private MapStateDescriptor<Relation, DirectFollowsObservation> directlyFollowsObservationsStateDescriptor = new MapStateDescriptor<>(
			"directlyFollowsObservationsState",
			TypeInformation.of(new TypeHint<Relation>() {
			}),
			TypeInformation.of(new TypeHint<DirectFollowsObservation>() {
			}));
	private MapStateDescriptor<TripleRelation, Observation> tripleFollowsObservationsStateDescriptor = new MapStateDescriptor<>(
			"tripleFollowsObservationsState",
			TypeInformation.of(new TypeHint<TripleRelation>() {
			}),
			TypeInformation.of(new TypeHint<Observation>() {
			}));
	private ListStateDescriptor<String> startActivitiesStateDescriptor = new ListStateDescriptor<>("startActivities",
			BasicTypeInfo.STRING_TYPE_INFO);

	private int bucketWidth;

	private final double dependencyThreshold;
	private final double andThreshold;
	private final double loopsLengthTwoThreshold;

	public HeuristicsMinerLossyCounting(
			double maxApproximationError,
			double dependencyThreshold,
			double andThreshold,
			double loopsLengthTwoThreshold) {
		this.dependencyThreshold = dependencyThreshold;
		this.andThreshold = andThreshold;
		this.loopsLengthTwoThreshold = loopsLengthTwoThreshold;

		this.bucketWidth = (int) (1.0 / maxApproximationError);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.startActivites = getRuntimeContext().getListState(startActivitiesStateDescriptor);
		this.activityObservations = getRuntimeContext().getMapState(activityObservationsStateDescriptor);
		this.caseObservations = getRuntimeContext().getMapState(caseObservationsStateDescriptor);
		this.directlyFollowsObservations = getRuntimeContext().getMapState(directlyFollowsObservationsStateDescriptor);
		this.tripleFollowsObservations = getRuntimeContext().getMapState(tripleFollowsObservationsStateDescriptor);
	}

	@Override
	public HeuristicNet ingest(FlatOcelEvent event) throws Exception {
		int currentBucket = (int) (this.getProcessedEvents() / bucketWidth);
		String activity = event.getType();
		Instant activtiyTime = event.getTime();
		String caseId = event.getProcessCase().getObjectId();

		// Update activity observations
		Observation activityObservation = this.activityObservations.get(activity);
		if (activityObservation == null) {
			activityObservation = new Observation(currentBucket - 1L);
		} else {
			activityObservation.incrementCount();
		}
		this.activityObservations.put(activity, activityObservation);

		// Update case observations
		CaseObservation caseObservation = this.caseObservations.get(caseId);
		if (caseObservation == null) {
			caseObservation = new CaseObservation(activity, activtiyTime, currentBucket - 1L);
			this.startActivites.add(activity);
		} else {
			Relation newRelation = new Relation(caseObservation.getLatestActivity(), activity);
			Instant lastActivityTime = caseObservation.getLatestActivityTime();

			// Update dependency Relation
			DirectFollowsObservation relationObservation = this.directlyFollowsObservations.get(newRelation);
			if (relationObservation == null) {
				relationObservation = new DirectFollowsObservation(currentBucket - 1L, Duration.between(lastActivityTime, activtiyTime));
			} else {
				relationObservation.incrementCount();
				if (Duration.between(lastActivityTime, activtiyTime).isNegative())
					throw new Error();
				relationObservation.addDuration(Duration.between(lastActivityTime, activtiyTime));
			}
			this.directlyFollowsObservations.put(newRelation, relationObservation);

			// Update triple dependency Relation if we have already seen two activities in
			// the current case
			if (caseObservation.getSecondLatestActivity() != null) {
				TripleRelation newTripleRelation = new TripleRelation(caseObservation.getSecondLatestActivity(),
						caseObservation.getLatestActivity(), activity);
				Observation tripleObservation = this.tripleFollowsObservations.get(newTripleRelation);
				if (tripleObservation == null) {
					tripleObservation = new Observation(currentBucket - 1L);
				} else {
					tripleObservation.incrementCount();
				}
				this.tripleFollowsObservations.put(newTripleRelation, tripleObservation);
			}
			caseObservation.incrementCount();
			caseObservation.setLatestActivity(activity, activtiyTime);
		}
		this.caseObservations.put(caseId, caseObservation);

		// Periodic cleanup
		if (this.getProcessedEvents() % bucketWidth == 0) {

			Iterator<Observation> activityObservationsIterator = this.activityObservations.values().iterator();
			while (activityObservationsIterator.hasNext()) {
				Observation entry = activityObservationsIterator.next();
				if (entry.getCount() + entry.getError() <= currentBucket) {
					// activityObservationsIterator.remove();
				}
			}

			Iterator<CaseObservation> caseObservationsIterator = this.caseObservations
					.values().iterator();
			while (caseObservationsIterator.hasNext()) {
				CaseObservation entry = caseObservationsIterator.next();
				if (entry.getCount() + entry.getError() <= currentBucket) {
					// caseObservationsIterator.remove();
				}
			}

			Iterator<DirectFollowsObservation> directlyFollowsIterator = this.directlyFollowsObservations.values()
					.iterator();
			while (directlyFollowsIterator.hasNext()) {
				Observation entry = directlyFollowsIterator.next();
				if (entry.getCount() + entry.getError() <= currentBucket) {
					// directlyFollowsIterator.remove();
				}
			}

			Iterator<Observation> tripleFollowsIterator = this.tripleFollowsObservations.values().iterator();
			while (tripleFollowsIterator.hasNext()) {
				Observation entry = tripleFollowsIterator.next();
				if (entry.getCount() + entry.getError() <= currentBucket) {
					// tripleFollowsIterator.remove();
				}
			}
		}

		if (this.getProcessedEvents() % 1 == 0) {

			Set<String> startActivities = new HashSet<>();
			for (String element : this.startActivites.get()) {
				startActivities.add(element);
			}

			Set<String> endActivities = new HashSet<>();
			Iterator<CaseObservation> caseObservationsIterator = this.caseObservations
					.values().iterator();
			while (caseObservationsIterator.hasNext()) {
				CaseObservation entry = caseObservationsIterator.next();
				endActivities.add(entry.getLatestActivity());
			}

			// Activity Occurences
			Map<String, Long> activityOccurences = new HashMap<>();
			Iterator<Map.Entry<String, Observation>> activityObservationsIterator = this.activityObservations
					.entries().iterator();
			while (activityObservationsIterator.hasNext()) {
				Map.Entry<String, Observation> entry = activityObservationsIterator.next();
				activityOccurences.put(entry.getKey(), entry.getValue().getCount());
			}

			// Direct Follows Relations
			Map<Relation, DirectFollowsObservation> directlyFollowsRelations = new HashMap<>();
			Iterator<Map.Entry<Relation, DirectFollowsObservation>> directlyFollowsIterator = this.directlyFollowsObservations
					.entries().iterator();
			while (directlyFollowsIterator.hasNext()) {
				Map.Entry<Relation, DirectFollowsObservation> entry = directlyFollowsIterator.next();
				directlyFollowsRelations.put(entry.getKey(), entry.getValue());
			}

			// Triple Relations
			Map<Relation, Long> tripleFollowsRelations = new HashMap<>();
			Iterator<Map.Entry<TripleRelation, Observation>> tripleFollowsIterator = this.tripleFollowsObservations
					.entries().iterator();
			while (tripleFollowsIterator.hasNext()) {
				Map.Entry<TripleRelation, Observation> entry = tripleFollowsIterator.next();
				TripleRelation triple = entry.getKey();
				if (triple.getSource().equals(triple.getTarget()) && !triple.getSource().equals(triple.getMiddle())) {
					Relation relation = new Relation(entry.getKey().getSource(), entry.getKey().getMiddle());
					tripleFollowsRelations.put(relation, entry.getValue().getCount());
				}
			}

			return new HeuristicNetBuilder(startActivities, endActivities, activityOccurences, directlyFollowsRelations,
					tripleFollowsRelations, dependencyThreshold, andThreshold, loopsLengthTwoThreshold).build();
		}

		return null;
	}
}
