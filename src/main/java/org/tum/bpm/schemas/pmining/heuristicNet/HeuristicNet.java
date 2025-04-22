package org.tum.bpm.schemas.pmining.heuristicNet;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.tum.bpm.schemas.pmining.ProcessMiningResponse;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class HeuristicNet extends ProcessMiningResponse {

	private Map<String, HeuristicNetNode> nodes;
	private Set<HeuristicNetEdge> edges;
	private Set<String> sourceNodes;
	private Set<String> sinkNodes;

	public HeuristicNet(Set<String> sourceNodes, Set<String> sinkNodes) {
		super();
		this.edges = new HashSet<>();
		this.nodes = new HashMap<>();
		this.sourceNodes = sourceNodes;
		this.sinkNodes = sinkNodes;
	}

	public void addNode(String activity, long occurences) {
		HeuristicNetNode node = new HeuristicNetNode(activity, occurences);
		this.nodes.put(activity, node);
	}

	public boolean containsNode(String activity) {
		return this.nodes.containsKey(activity);
	}

	public void addBinding(String source, String target, Duration duration, double dependencyValue, long dfgValue) {
		HeuristicNetNode sourceNode = this.nodes.get(source);
		HeuristicNetNode targetNode = this.nodes.get(target);

		if (sourceNode != null && targetNode != null) {
			HeuristicNetEdge edge = new HeuristicNetEdge(source, target, duration, dfgValue, dependencyValue);
			this.edges.add(edge);

			sourceNode.addOutputBinding(targetNode.getActivity());
			targetNode.addInputBinding(sourceNode.getActivity());
		}
	}

	public Collection<HeuristicNetNode> getNodes() {
		return this.nodes.values();
	}

	public HeuristicNetNode getNode(String activity) {
		return this.nodes.get(activity);
	}

	@Override
	public String toString() {
		ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        try {
			String json = objectMapper.writeValueAsString(this);
			return json;
		} catch (JsonProcessingException e) {
			return super.toString();
		}
	}
}