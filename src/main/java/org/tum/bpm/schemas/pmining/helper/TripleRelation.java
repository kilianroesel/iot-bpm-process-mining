package org.tum.bpm.schemas.pmining.helper;

import lombok.Getter;

@Getter
public class TripleRelation extends Relation {

    private String middle;

    public TripleRelation(String source, String middle, String target) {
        super(source, target);
        this.middle = middle;
    }
    
}
