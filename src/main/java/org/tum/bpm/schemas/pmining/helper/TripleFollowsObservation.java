package org.tum.bpm.schemas.pmining.helper;

import lombok.Getter;

@Getter
public class TripleFollowsObservation extends Observation {
    
    private String source;
    private String middle;
    private String target;

    public TripleFollowsObservation(String source, String middle, String target, long error) {
        super(error);
        this.source = source;
        this.middle = middle;
        this.target = target;
    }
}
