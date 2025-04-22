package org.tum.bpm.schemas.pmining.heuristicNet;

import java.time.Duration;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HeuristicNetEdge {

	private String sourceNode;
	private String targetNode;
    private Duration duration;
    private long dfgValue;
    private double dependencyValue;

	@Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        HeuristicNetEdge edge = (HeuristicNetEdge) obj;
        return Objects.equals(this.sourceNode, edge.sourceNode) && Objects.equals(this.targetNode, edge.targetNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sourceNode, this.targetNode);
    }
}