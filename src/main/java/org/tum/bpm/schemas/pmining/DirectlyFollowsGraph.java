package org.tum.bpm.schemas.pmining;

import org.tum.bpm.schemas.pmining.helper.Relation;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Data
@EqualsAndHashCode(callSuper=true)
public class DirectlyFollowsGraph extends ProcessMiningResponse {

	private Map<Relation, Long> directlyFollowsRelation;
	private Map<String, Long> startingActivities;
	private Map<String, Long> endingActivities;

	public DirectlyFollowsGraph(Map<Relation, Long> directlyFollowsRelation, Map<String, Long> startingActivities, Map<String, Long> endingActivities) {
		this.directlyFollowsRelation = directlyFollowsRelation;
		this.startingActivities = startingActivities;
		this.endingActivities = endingActivities;
	}
}