package org.tum.bpm.schemas.pmining.heuristicNet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.tum.bpm.schemas.pmining.helper.DirectFollowsObservation;
import org.tum.bpm.schemas.pmining.helper.Relation;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HeuristicNetBuilder {

    // Inputs for heuristic net creation
    private final Set<String> startActivities;
    private final Set<String> endActivities;
    private final Map<String, Long> activityOccurences;
    private final Map<Relation, DirectFollowsObservation> directlyFollowsFrequency;
    private final Map<Relation, Long> tripleFollowsFrequency;

    // Parameters for calculating the heuristic net
    private final double dependencyThreshold;
    private final double andMeasurementThreshold;
    private final double loopsLengthTwoThreshold;

    // Calculated from directly follows frequency
    private Map<Relation, Double> dependencyRelations = new HashMap<Relation, Double>();

    public HeuristicNetBuilder(Set<String> startActivities, Set<String> endActivities,
            Map<String, Long> activityOccurences, Map<Relation, DirectFollowsObservation> directlyFollowsFrequency,
            Map<Relation, Long> tripleFollowsFrequency, double dependencyThreshold,
            double andMeasurementThreshold, double loopsLengthTwoThreshold) {
        this.startActivities = startActivities;
        this.endActivities = endActivities;
        this.activityOccurences = activityOccurences;
        this.directlyFollowsFrequency = directlyFollowsFrequency;
        this.tripleFollowsFrequency = tripleFollowsFrequency;
        this.dependencyThreshold = dependencyThreshold;
        this.andMeasurementThreshold = andMeasurementThreshold;
        this.loopsLengthTwoThreshold = loopsLengthTwoThreshold;
        this.calculateDependencyRelations();
    }

    public HeuristicNet build() {
        HeuristicNet heuNet = this.createHeuristicNet();
        return heuNet;
    }

    private void calculateDependencyRelations() {

        Map<Relation, Double> dependencyRelations = new HashMap<>();

        for (Map.Entry<Relation, DirectFollowsObservation> directlyFollowsFrequency : this.directlyFollowsFrequency.entrySet()) {
            Relation relation = directlyFollowsFrequency.getKey();
            Long frequency = directlyFollowsFrequency.getValue().getCount();

            Double dependency;
            if (relation.getSource().equals(relation.getTarget())) {
                dependency = (double) frequency / (double) (frequency + 1);
            } else {
                Relation inverseRelation = new Relation(relation.getTarget(), relation.getSource());
                DirectFollowsObservation inverseObservation = this.directlyFollowsFrequency.get(inverseRelation);
                if (inverseObservation != null) {
                    long inverseCount = inverseObservation.getCount();
                    dependency = (double) (frequency - inverseCount) / (double) (frequency + inverseCount + 1);
                } else {
                    dependency = (double) frequency / (double) (frequency + 1);
                }
            }
            dependencyRelations.put(relation, dependency);
        }
        this.dependencyRelations = dependencyRelations;
    }

    private HeuristicNet createHeuristicNet() {
        HeuristicNet heuristicNet = new HeuristicNet(this.startActivities,
                this.endActivities);

        for (Map.Entry<Relation, Double> dependencyRelation : this.dependencyRelations.entrySet()) {
            // TODO: Refine with additional conditions
            if (dependencyRelation.getValue() >= this.dependencyThreshold) {
                String sourceNode = dependencyRelation.getKey().getSource();
                String targetNode = dependencyRelation.getKey().getTarget();

                if (!heuristicNet.containsNode(sourceNode)) {
                    heuristicNet.addNode(sourceNode, this.activityOccurences.getOrDefault(sourceNode, 0L));
                }

                if (!heuristicNet.containsNode(targetNode)) {
                    heuristicNet.addNode(targetNode, this.activityOccurences.getOrDefault(targetNode, 0L));
                }

                DirectFollowsObservation dfgObservation = this.directlyFollowsFrequency.get(dependencyRelation.getKey());
                heuristicNet.addBinding(sourceNode, targetNode, dfgObservation.getAverageDuration(), dependencyRelation.getValue(), dfgObservation.getCount());
            }
        }

        Collection<HeuristicNetNode> nodes = heuristicNet.getNodes();
        for (HeuristicNetNode node : nodes) {
            this.calculateAndMeasureIn(node);
            this.calculateAndMeasureOut(node);
        }

        // Calculate Loops Length Two
        for (Map.Entry<Relation, Long> tripleRelation : this.tripleFollowsFrequency.entrySet()) {
            Relation relation = tripleRelation.getKey();
            HeuristicNetNode node = heuristicNet.getNode(relation.getSource());
            if (node != null) {
                DirectFollowsObservation dfgObservation = this.directlyFollowsFrequency.get(tripleRelation.getKey());
                long dfgValue = dfgObservation != null ? dfgObservation.getCount() : 0L;
                long v1 = tripleRelation.getValue();
                Pair<String, String> inverseRelation = new ImmutablePair<>(relation.getSource(), relation.getTarget());
                long v2 = this.tripleFollowsFrequency.getOrDefault(inverseRelation, 0L);
                double l2l = (double) (v1 + v2) / (v1 + v2 + 1);
                if (l2l >= this.loopsLengthTwoThreshold) {
                    node.putLoopLengthTwo(relation.getTarget(), dfgValue);
                }
            }
        }

        return heuristicNet;
    }

    private void calculateAndMeasureIn(HeuristicNetNode node) {
        List<String> inputBindings = node.getInputBindings();
        Map<Relation, Double> andMeasurementsIn = new HashMap<Relation, Double>();
        for (int i = 0; i < inputBindings.size(); i++) {
            for (int j = i + 1; j < inputBindings.size(); j++) {
                String node1 = inputBindings.get(i);
                String node2 = inputBindings.get(j);

                DirectFollowsObservation dfgObservation1 = this.directlyFollowsFrequency.get(new Relation(node1, node2));
                long dfgValue1 = dfgObservation1 != null ? dfgObservation1.getCount() : 0L;
          
                DirectFollowsObservation dfgObservation2 = this.directlyFollowsFrequency.get(new Relation(node2, node1));
                long dfgValue2 = dfgObservation2 != null ? dfgObservation2.getCount() : 0L;

                DirectFollowsObservation dfgObservation3 = this.directlyFollowsFrequency.get(new Relation(node1, node.getActivity()));
                long dfgValue3 = dfgObservation3 != null ? dfgObservation3.getCount() : 0L;

                DirectFollowsObservation dfgObservation4 = this.directlyFollowsFrequency.get(new Relation(node2, node.getActivity()));
                long dfgValue4 = dfgObservation4 != null ? dfgObservation4.getCount() : 0L;

                double andMeasurement = (double) (dfgValue1 + dfgValue2) / (dfgValue3 + dfgValue4 + 1);
                if (andMeasurement >= this.andMeasurementThreshold) {
                    Relation edge = new Relation(node1, node2);
                    andMeasurementsIn.put(edge, andMeasurement);
                }
            }
        }
        node.setAndMeasurementsIn(andMeasurementsIn);
    }

    private void calculateAndMeasureOut(HeuristicNetNode node) {
        List<String> outputBindings = node.getOutputBindings();
        Map<Relation, Double> andMeasurementsOut = new HashMap<Relation, Double>();
        for (int i = 0; i < outputBindings.size(); i++) {
            for (int j = i + 1; j < outputBindings.size(); j++) {
                String node1 = outputBindings.get(i);
                String node2 = outputBindings.get(j);

                DirectFollowsObservation dfgObservation1 = this.directlyFollowsFrequency.get(new Relation(node1, node2));
                long dfgValue1 = dfgObservation1 != null ? dfgObservation1.getCount() : 0L;
          
                DirectFollowsObservation dfgObservation2 = this.directlyFollowsFrequency.get(new Relation(node2, node1));
                long dfgValue2 = dfgObservation2 != null ? dfgObservation2.getCount() : 0L;

                DirectFollowsObservation dfgObservation3 = this.directlyFollowsFrequency.get(new Relation(node1, node.getActivity()));
                long dfgValue3 = dfgObservation3 != null ? dfgObservation3.getCount() : 0L;

                DirectFollowsObservation dfgObservation4 = this.directlyFollowsFrequency.get(new Relation(node2, node.getActivity()));
                long dfgValue4 = dfgObservation4 != null ? dfgObservation4.getCount() : 0L;

                double andMeasurement = (double) (dfgValue1 + dfgValue2) / (dfgValue3 + dfgValue4 + 1);
                if (andMeasurement >= this.andMeasurementThreshold) {
                    Relation edge = new Relation(node1, node2);
                    andMeasurementsOut.put(edge, andMeasurement);
                }
            }
        }
        node.setAndMeasurementsOut(andMeasurementsOut);
    }
}
