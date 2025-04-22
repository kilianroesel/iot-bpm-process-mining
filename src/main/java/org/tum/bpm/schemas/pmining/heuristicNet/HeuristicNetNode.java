package org.tum.bpm.schemas.pmining.heuristicNet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.tum.bpm.schemas.pmining.helper.Relation;

import lombok.Data;

@Data
public class HeuristicNetNode {

	private String activity;
	private long occurences;
	private List<String> inputBindings;
	private List<String> outputBindings;
	@JsonIgnore
	private Map<Relation, Double> andMeasurementsOut;
	@JsonIgnore
	private Map<Relation, Double> andMeasurementsIn;
	@JsonIgnore
	private Map<String, Double> loopsLengthTwo;


	public HeuristicNetNode(String activity, long occurences) {
		this.activity = activity;
		this.occurences = occurences;
		this.inputBindings = new ArrayList<>();
		this.outputBindings = new ArrayList<>();
		this.andMeasurementsOut = new HashMap<>();
		this.andMeasurementsIn = new HashMap<>();
		this.loopsLengthTwo = new HashMap<>();
	}

	public boolean addInputBinding(String sinkNode) {
		return this.inputBindings.add(sinkNode);
	}

	public boolean addOutputBinding(String outputBinding) {
		return this.outputBindings.add(outputBinding);
	}

	public void putLoopLengthTwo(String activity, double value) {
		this.loopsLengthTwo.put(activity, value);
	}
}